import { Injectable, NotFoundException } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue, Job } from 'bullmq';
import { TRANSACTION_QUEUE_NAME, CreateTransactionJobDto } from '../queue/transaction.queue';

@Injectable()
export class TransactionService {
  constructor(
    @InjectQueue(TRANSACTION_QUEUE_NAME)
    private readonly transactionQueue: Queue,
  ) {}

  /**
   * Adds a transaction job to the BullMQ queue and returns the job details.
   */
  async addTransactionToQueue(dto: CreateTransactionJobDto): Promise<{ message: string; jobId: string }> {
    const job: Job = await this.transactionQueue.add('send-token', dto);

    return {
      message: 'Transaction added to queue',
      jobId: job.id as string,
    };
  }

  /**
   * Retrieves transaction job details and processing state by jobId.
   */
  async getTransactionStatus(jobId: string) {
    const job = await this.transactionQueue.getJob(jobId);
    if (!job) {
      throw new NotFoundException(`Transaction job with ID ${jobId} not found`);
    }

    const state = await job.getState();
    const isCompleted = state === 'completed';
    const isFailed = state === 'failed';

    return {
      jobId: job.id,
      state,
      data: job.data,
      returnvalue: isCompleted ? job.returnvalue : null,
      failedReason: isFailed ? job.failedReason : null,
      attemptsMade: job.attemptsMade,
      timestamp: new Date(job.timestamp).toISOString(),
      processedOn: job.processedOn ? new Date(job.processedOn).toISOString() : null,
      finishedOn: job.finishedOn ? new Date(job.finishedOn).toISOString() : null,
    };
  }

  /**
   * Retrieves high-level queue metrics (waiting, active, completed, failed counts).
   */
  async getQueueStatus() {
    const [waiting, active, completed, failed, delayed] = await Promise.all([
      this.transactionQueue.getWaitingCount(),
      this.transactionQueue.getActiveCount(),
      this.transactionQueue.getCompletedCount(),
      this.transactionQueue.getFailedCount(),
      this.transactionQueue.getDelayedCount(),
    ]);

    return {
      queueName: TRANSACTION_QUEUE_NAME,
      status: 'ONLINE',
      metrics: {
        waiting,
        active,
        completed,
        failed,
        delayed,
        total: waiting + active + completed + failed + delayed,
      },
    };
  }
}
