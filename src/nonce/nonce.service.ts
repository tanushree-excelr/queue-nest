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
   * Records a completed or failed transaction result without calculating nonces.
   */
  async recordTransaction(
    walletAddress: string,
    nonce: number | null,
    status: NonceStatus,
    transactionHash?: string,
  ): Promise<NonceEntity> {
    try {
      const record = this.nonceRepository.create({
        walletAddress,
        nonce: nonce ?? undefined,
        status,
        transactionHash,
      });

      const saved = await this.nonceRepository.save(record);
      this.logger.log(
        `Recorded transaction for wallet ${walletAddress} (Nonce: ${nonce ?? 'N/A'}, status: ${status}, txHash: ${transactionHash || 'N/A'})`,
      );
      return saved;
    } catch (error) {
      this.logger.error(`Failed to record transaction for wallet ${walletAddress}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Updates status and optional transactionHash for a reserved nonce record.
   */
  async updateNonceStatus(
    id: number,
    status: NonceStatus,
    transactionHash?: string,
  ): Promise<NonceEntity> {
    const nonceRecord = await this.nonceRepository.findOne({ where: { id } });
    if (!nonceRecord) {
      throw new Error(`Nonce record with ID ${id} not found.`);
    }

    nonceRecord.status = status;
    if (transactionHash) {
      nonceRecord.transactionHash = transactionHash;
    }

    const updated = await this.nonceRepository.save(nonceRecord);
    this.logger.log(
      `Updated nonce #${updated.nonce} for wallet ${updated.walletAddress} to status: ${status} (txHash: ${transactionHash || 'N/A'})`,
    );

    return updated;
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
