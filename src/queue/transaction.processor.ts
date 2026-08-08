import { Processor, WorkerHost } from '@nestjs/bullmq';
import { Logger } from '@nestjs/common';
import { Job } from 'bullmq';
import { NonceService } from '../nonce/nonce.service';
import { NonceStatus } from '../nonce/nonce.entity';
import { BlockchainService } from '../blockchain/blockchain.service';
import { TRANSACTION_QUEUE_NAME, CreateTransactionJobDto } from './transaction.queue';

@Processor(TRANSACTION_QUEUE_NAME, {
  concurrency: 1,
})
export class TransactionProcessor extends WorkerHost {
  private readonly logger = new Logger(TransactionProcessor.name);

  constructor(
    private readonly nonceService: NonceService,
    private readonly blockchainService: BlockchainService,
  ) {
    super();
  }

  async process(job: Job<CreateTransactionJobDto>): Promise<any> {
    const { toWallet, amount } = job.data;
    const fromWallet = this.blockchainService.getWalletAddress();

    this.logger.log(`[WORKER] Starting ${job.id}`);

    try {
      const result = await this.blockchainService.sendTransaction(
        toWallet,
        amount,
        String(job.id),
      );

      await this.nonceService.recordTransaction(
        fromWallet,
        result.nonce,
        NonceStatus.COMPLETED,
        result.transactionHash,
      );

      this.logger.log(`[WORKER] Completed ${job.id}`);

      return {
        success: true,
        nonce: result.nonce,
        transactionHash: result.transactionHash,
        completedAt: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`[WORKER] Job ${job.id} failed: ${error.message}`);
      await this.nonceService
        .recordTransaction(fromWallet, null, NonceStatus.FAILED)
        .catch(() => null);

      throw error;
    }
  }
}
