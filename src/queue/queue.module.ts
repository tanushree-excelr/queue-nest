import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { TRANSACTION_QUEUE_NAME } from './transaction.queue';
import { TransactionProcessor } from './transaction.processor';
import { NonceModule } from '../nonce/nonce.module';
import { BlockchainModule } from '../blockchain/blockchain.module';

const redisHost = process.env.REDIS_HOST || 'localhost';
const redisPort = parseInt(process.env.REDIS_PORT || '6379', 10);
const redisPassword = process.env.REDIS_PASSWORD || 'secret';

@Module({
  imports: [
    BullModule.forRoot({
      connection: {
        host: redisHost,
        port: redisPort,
        password: redisPassword,
        maxRetriesPerRequest: null,
        enableOfflineQueue: false,
        connectTimeout: 5000,
      },
    }),
    BullModule.registerQueue({
      name: TRANSACTION_QUEUE_NAME,
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000,
        },
        removeOnComplete: false,
        removeOnFail: false,
      },
    }),
    NonceModule,
    BlockchainModule,
  ],
  providers: [TransactionProcessor],
  exports: [BullModule],
})
export class QueueModule {}
