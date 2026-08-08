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
    const jobIdStr = String(job.id);

    this.logger.log(`[Worker] Received job: ${jobIdStr}`);
    this.logger.log(`[Worker] Wallet: ${fromWallet}`);

    // Mark status as PROCESSING in DB
    await this.nonceService
      .recordTransaction(
        jobIdStr,
        fromWallet,
        toWallet,
        amount,
        null,
        NonceStatus.PROCESSING,
      )
      .catch(() => null);

    this.logger.log(`[Worker] Fetching pending nonce from provider...`);

    try {
      const nonce = await this.blockchainService.getPendingNonce(fromWallet);
      this.logger.log(`[Worker] Provider returned nonce: ${nonce}`);
      this.logger.log(`[Worker] Sending transaction with nonce: ${nonce}`);

      const result = await this.blockchainService.sendTransaction(
        toWallet,
        amount,
        nonce,
      );

      this.logger.log(`[Worker] Transaction hash: ${result.transactionHash}`);

      await this.nonceService.recordTransaction(
        jobIdStr,
        fromWallet,
        toWallet,
        amount,
        nonce,
        NonceStatus.CONFIRMED,
        result.transactionHash,
      );

      this.logger.log(`[Worker] Database updated for job: ${jobIdStr}`);
      this.logger.log(`[Worker] Job completed: ${jobIdStr}`);

      return {
        success: true,
        nonce: result.nonce,
        transactionHash: result.transactionHash,
        completedAt: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`[Worker] Job ${jobIdStr} failed: ${error.message}`);
      await this.nonceService
        .recordTransaction(
          jobIdStr,
          fromWallet,
          toWallet,
          amount,
          null,
          NonceStatus.FAILED,
        )
        .catch(() => null);

      throw error;
    }
  }
}
