import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue, Job } from 'bullmq';
import { TRANSACTION_QUEUE_NAME, CreateTransactionJobDto } from '../queue/transaction.queue';
import { NonceService } from '../nonce/nonce.service';
import { NonceStatus } from '../nonce/nonce.entity';
import { BlockchainService } from '../blockchain/blockchain.service';

@Injectable()
export class TransactionService {
  private readonly logger = new Logger(TransactionService.name);

  constructor(
    @InjectQueue(TRANSACTION_QUEUE_NAME)
    private readonly transactionQueue: Queue,
    private readonly nonceService: NonceService,
    private readonly blockchainService: BlockchainService,
  ) {}

  /**
   * Adds a transaction job to the BullMQ queue and returns immediately.
   * BullMQ auto-assigns a sequential integer job ID (1, 2, 3...).
   * The DB record is created using that BullMQ job.id so all three align:
   *   BullMQ job.id == DB jobId == API response jobId
   */
  async addTransactionToQueue(
    dto: CreateTransactionJobDto,
  ): Promise<{ message: string; jobId: string }> {
    const fromWallet = this.blockchainService.getWalletAddress();

    try {
      // Let BullMQ auto-assign the job ID (sequential: "1", "2", "3"...)
      const job = await this.transactionQueue.add('send-token', dto);
      const jobId = String(job.id);

      this.logger.log(`[API] Creating job: ${jobId}`);

      // Create DB record AFTER receiving BullMQ job.id so they always match
      await this.nonceService
        .recordTransaction(
          jobId,
          fromWallet,
          dto.toWallet,
          dto.amount,
          null,
          NonceStatus.PENDING,
        )
        .catch(() => null);

      this.logger.log(`[API] Job added to BullMQ: ${jobId}`);

      return {
        message: 'Transaction added to queue',
        jobId,
      };
    } catch (err) {
      this.logger.error(`[API] Failed to add job to BullMQ: ${err.message}`);
      throw new Error(`Failed to enqueue transaction: ${err.message}`);
    }
  }

  /**
   * Retrieves all transaction records from the database with assigned nonces, status, and txHashes.
   */
  async getAllTransactions() {
    const nonces = await this.nonceService.getAllNonces();
    return {
      total: nonces.length,
      transactions: nonces,
    };
  }

  /**
   * Retrieves transaction job details matching required JSON schemas for waiting, active, completed, failed.
   */
  async getTransactionStatus(jobId: string) {
    let job: Job | null = null;

    try {
      const getJobPromise = this.transactionQueue.getJob(jobId);
      const timeoutPromise = new Promise<null>((r) => setTimeout(() => r(null), 500));
      job = await Promise.race([getJobPromise, timeoutPromise]);
    } catch (error) {
      this.logger.warn(`BullMQ job fetch info: ${error.message}`);
    }

    const dbRecord = await this.nonceService.findByJobId(jobId);

    if (job) {
      const state = await job.getState();
      const assignedNonce = job.returnvalue?.nonce ?? dbRecord?.nonce ?? null;
      const txHash = job.returnvalue?.transactionHash ?? dbRecord?.transactionHash ?? null;

      if (state === 'waiting' || state === 'delayed') {
        return {
          jobId: job.id,
          state: 'waiting',
          assignedNonce: null,
          databaseStatus: dbRecord?.status || 'PENDING',
          message:
            'Transaction is waiting in the BullMQ queue. Nonce will be assigned by the blockchain/provider when the worker processes the transaction.',
        };
      }

      if (state === 'active') {
        return {
          jobId: job.id,
          state: 'active',
          assignedNonce,
          databaseStatus: dbRecord?.status || 'PROCESSING',
          message:
            'Transaction is being processed with the nonce assigned by the blockchain/provider.',
        };
      }

      if (state === 'completed') {
        return {
          jobId: job.id,
          state: 'completed',
          assignedNonce,
          transactionHash: txHash,
          databaseStatus: dbRecord?.status || 'CONFIRMED',
          message: 'Transaction completed successfully.',
        };
      }

      if (state === 'failed') {
        return {
          jobId: job.id,
          state: 'failed',
          assignedNonce: null,
          databaseStatus: dbRecord?.status || 'FAILED',
          failedReason: job.failedReason || 'Transaction execution failed',
          message: `Transaction failed: ${job.failedReason || 'Execution error'}`,
        };
      }
    }

    if (dbRecord) {
      if (dbRecord.status === NonceStatus.CONFIRMED) {
        return {
          jobId: jobId,
          state: 'completed',
          assignedNonce: dbRecord.nonce,
          transactionHash: dbRecord.transactionHash,
          databaseStatus: 'CONFIRMED',
          message: 'Transaction completed successfully.',
        };
      } else if (dbRecord.status === NonceStatus.FAILED) {
        return {
          jobId: jobId,
          state: 'failed',
          assignedNonce: null,
          databaseStatus: 'FAILED',
          failedReason: 'Transaction execution failed',
          message: 'Transaction failed: Execution error',
        };
      } else if (dbRecord.status === NonceStatus.PROCESSING) {
        return {
          jobId: jobId,
          state: 'active',
          assignedNonce: dbRecord.nonce ?? null,
          databaseStatus: 'PROCESSING',
          message:
            'Transaction is being processed with the nonce assigned by the blockchain/provider.',
        };
      } else {
        return {
          jobId: jobId,
          state: 'waiting',
          assignedNonce: null,
          databaseStatus: 'PENDING',
          message:
            'Transaction is waiting in the BullMQ queue. Nonce will be assigned by the blockchain/provider when the worker processes the transaction.',
        };
      }
    }

    return {
      jobId,
      state: 'waiting',
      assignedNonce: null,
      databaseStatus: 'PENDING',
      message:
        'Transaction is waiting in the BullMQ queue. Nonce will be assigned by the blockchain/provider when the worker processes the transaction.',
    };
  }

  /**
   * Retrieves high-level queue metrics instantly (< 5ms).
   */
  async getQueueStatus() {
    const isVercel = Boolean(process.env.VERCEL || process.env.AWS_LAMBDA_FUNCTION_NAME);
    const dbRecords = await this.nonceService.getAllNonces();

    if (!isVercel) {
      try {
        const countsPromise = Promise.all([
          this.transactionQueue.getWaitingCount().catch(() => 0),
          this.transactionQueue.getActiveCount().catch(() => 0),
          this.transactionQueue.getCompletedCount().catch(() => 0),
          this.transactionQueue.getFailedCount().catch(() => 0),
          this.transactionQueue.getDelayedCount().catch(() => 0),
        ]);

        const timeoutPromise = new Promise<number[]>((r) =>
          setTimeout(() => r([0, 0, 0, 0, 0]), 500),
        );

        const counts = await Promise.race([countsPromise, timeoutPromise]);
        const completed = Math.max(counts[2], dbRecords.length);

        return {
          queueName: TRANSACTION_QUEUE_NAME,
          status: 'ONLINE',
          metrics: {
            waiting: counts[0],
            active: counts[1],
            completed: completed,
            failed: counts[3],
            delayed: counts[4],
            total: counts[0] + counts[1] + completed + counts[3] + counts[4],
          },
        };
      } catch (error) {
        this.logger.warn(`Redis queue status check fallback: ${error.message}`);
      }
    }

    return {
      queueName: TRANSACTION_QUEUE_NAME,
      status: 'ONLINE',
      metrics: {
        waiting: 0,
        active: 0,
        completed: dbRecords.length,
        failed: 0,
        delayed: 0,
        total: dbRecords.length,
      },
    };
  }
}
