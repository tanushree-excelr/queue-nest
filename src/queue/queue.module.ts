import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import { TRANSACTION_QUEUE_NAME } from './transaction.queue';
import { TransactionProcessor } from './transaction.processor';
import { NonceModule } from '../nonce/nonce.module';
import { BlockchainModule } from '../blockchain/blockchain.module';

@Module({
  imports: [
    BullModule.forRoot({
      connection: {
        host: process.env.REDIS_HOST || 'localhost',
        port: parseInt(process.env.REDIS_PORT || '6379', 10),
        password: process.env.REDIS_PASSWORD || 'secret',
      },
    }),
    BullModule.registerQueue({
      name: TRANSACTION_QUEUE_NAME,
      defaultJobOptions: {
        attempts: 3,
        backoff: {
          type: 'exponential',
          delay: 2000, // 2s, 4s, 8s backoff
        },
        removeOnComplete: false, // Keep logs for query/monitoring APIs
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
