import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, DataSource } from 'typeorm';
import { NonceEntity, NonceStatus } from './nonce.entity';

@Injectable()
export class NonceService {
  private readonly logger = new Logger(NonceService.name);

  constructor(
    @InjectRepository(NonceEntity)
    private readonly nonceRepository: Repository<NonceEntity>,
    private readonly dataSource: DataSource,
  ) {}

  /**
   * Atomically reserves the next unique nonce for a given wallet address.
   * Compares the database's max reserved nonce with the live network nonce to ensure no collisions.
   */
  async reserveNextNonce(walletAddress: string, networkNonce: number = 0): Promise<NonceEntity> {
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      // Find current max nonce reserved in database for this wallet
      const maxNonceResult = await queryRunner.manager
        .createQueryBuilder(NonceEntity, 'nonce')
        .select('MAX(nonce.nonce)', 'maxNonce')
        .where('nonce.walletAddress = :walletAddress', { walletAddress })
        .getRawOne();

      const dbMaxNonce = maxNonceResult?.maxNonce != null ? Number(maxNonceResult.maxNonce) : -1;
      
      // Calculate next nonce: must be greater than both DB max nonce and live pending network nonce
      const nextNonce = Math.max(dbMaxNonce + 1, networkNonce);

      this.logger.log(
        `Reserving nonce ${nextNonce} for wallet ${walletAddress} (DB Max: ${dbMaxNonce}, Network Nonce: ${networkNonce})`,
      );

      const newNonce = queryRunner.manager.create(NonceEntity, {
        walletAddress,
        nonce: nextNonce,
        status: NonceStatus.PENDING,
      });

      const savedNonce = await queryRunner.manager.save(newNonce);
      await queryRunner.commitTransaction();

      return savedNonce;
    } catch (error) {
      await queryRunner.rollbackTransaction();
      this.logger.error(`Failed to reserve nonce for wallet ${walletAddress}: ${error.message}`, error.stack);
      throw error;
    } finally {
      await queryRunner.release();
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
   * Helper method to fetch all nonces recorded for a specific wallet address.
   */
  async getWalletNonces(walletAddress: string): Promise<NonceEntity[]> {
    return this.nonceRepository.find({
      where: { walletAddress },
      order: { nonce: 'ASC' },
    });
  }
}
