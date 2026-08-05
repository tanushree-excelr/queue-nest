import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { NonceEntity } from './nonce/nonce.entity';
import { NonceModule } from './nonce/nonce.module';
import { BlockchainModule } from './blockchain/blockchain.module';
import { QueueModule } from './queue/queue.module';
import { TransactionModule } from './transaction/transaction.module';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    TypeOrmModule.forRoot({
      type: 'sqlite',
      database: 'database.sqlite',
      entities: [NonceEntity],
      synchronize: true, // Auto-synchronize tables for local demo environment
    }),
    NonceModule,
    BlockchainModule,
    QueueModule,
    TransactionModule,
  ],
})
export class AppModule {}
