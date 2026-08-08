import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { NonceEntity, NonceStatus } from './nonce.entity';

@Injectable()
export class NonceService {
  private readonly logger = new Logger(NonceService.name);

  constructor(
    @InjectRepository(NonceEntity)
    private readonly nonceRepository: Repository<NonceEntity>,
  ) {}

  /**
   * Records or updates a transaction result with its provider-assigned nonce.
   */
  async recordTransaction(
    jobId: string,
    walletAddress: string,
    toWallet: string,
    amount: number,
    nonce: number | null,
    status: NonceStatus,
    transactionHash?: string,
  ): Promise<NonceEntity> {
    try {
      let record = await this.nonceRepository.findOne({ where: { jobId } });
      if (record) {
        record.status = status;
        if (nonce !== null && nonce !== undefined) record.nonce = nonce;
        if (transactionHash) record.transactionHash = transactionHash;
      } else {
        record = this.nonceRepository.create({
          jobId,
          walletAddress,
          toWallet,
          amount,
          nonce: nonce ?? undefined,
          status,
          transactionHash,
        });
      }

      const saved = await this.nonceRepository.save(record);
      this.logger.log(
        `Recorded transaction ${jobId} for wallet ${walletAddress} (Nonce: ${nonce ?? 'N/A'}, status: ${status}, txHash: ${transactionHash || 'N/A'})`,
      );
      return saved;
    } catch (error) {
      this.logger.error(`Failed to record transaction for wallet ${walletAddress}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Finds a recorded transaction by BullMQ jobId.
   */
  async findByJobId(jobId: string): Promise<NonceEntity | null> {
    return this.nonceRepository.findOne({ where: { jobId } });
  }

  /**
   * Creates a PENDING record only if one does not already exist for this jobId.
   * This prevents the API's initial DB write from overwriting the worker's PROCESSING write
   * when the worker picks up the job before the API has finished writing.
   */
  async createPendingIfAbsent(
    jobId: string,
    walletAddress: string,
    toWallet: string,
    amount: number,
  ): Promise<NonceEntity> {
    const existing = await this.nonceRepository.findOne({ where: { jobId } });
    if (existing) {
      this.logger.log(`[API] DB record for job ${jobId} already exists (status: ${existing.status}) — skipping PENDING write`);
      return existing;
    }
    const record = this.nonceRepository.create({
      jobId,
      walletAddress,
      toWallet,
      amount,
      nonce: undefined,
      status: NonceStatus.PENDING,
    });
    const saved = await this.nonceRepository.save(record);
    this.logger.log(`[API] DB record created for job ${jobId} (status: PENDING)`);
    return saved;
  }

  /**
   * Fetches all recorded nonces across all wallets, sorted by latest first.
   */
  async getAllNonces(): Promise<NonceEntity[]> {
    return this.nonceRepository.find({
      order: { id: 'DESC' },
    });
  }

  /**
   * Helper method to fetch all nonces recorded for a specific wallet address.
   */
  async getWalletNonces(walletAddress: string): Promise<NonceEntity[]> {
    return this.nonceRepository.find({
      where: { walletAddress },
      order: { nonce: 'ASC' },
    });
  }
}
