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

    this.logger.log(
      `[Queue Worker] Processing Job ID #${job.id} (Attempt ${job.attemptsMade + 1}) - Sending ${amount} tokens from ${fromWallet} to ${toWallet}`,
    );

    let reservedNonce;
    try {
      const networkNonce = await this.blockchainService.getNetworkNonce(fromWallet);

      reservedNonce = await this.nonceService.reserveNextNonce(fromWallet, networkNonce);
      this.logger.log(`[Queue Worker] Nonce ${reservedNonce.nonce} assigned to Job #${job.id}`);

      const result = await this.blockchainService.sendTransaction(
        toWallet,
        amount,
        reservedNonce.nonce,
      );

      await this.nonceService.updateNonceStatus(
        reservedNonce.id,
        NonceStatus.COMPLETED,
        result.transactionHash,
      );

      this.logger.log(`[Queue Worker] Job #${job.id} completed successfully. TX Hash: ${result.transactionHash}`);

      return {
        success: true,
        nonce: reservedNonce.nonce,
        transactionHash: result.transactionHash,
        completedAt: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(`[Queue Worker] Error processing Job #${job.id}: ${error.message}`);

      if (reservedNonce) {
        await this.nonceService.updateNonceStatus(reservedNonce.id, NonceStatus.FAILED);
      }

      throw error;
    }
  }
}
