import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { NonceEntity } from './nonce/nonce.entity';
import { NonceModule } from './nonce/nonce.module';
import { BlockchainModule } from './blockchain/blockchain.module';
import { QueueModule } from './queue/queue.module';
import { TransactionModule } from './transaction/transaction.module';
import * as path from 'path';

const isVercel = Boolean(process.env.VERCEL || process.env.AWS_LAMBDA_FUNCTION_NAME);
const dbPath = isVercel ? path.join('/tmp', 'database.sqlite') : 'database.sqlite';
const databaseUrl = process.env.DATABASE_URL || process.env.POSTGRES_URL;

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
    TypeOrmModule.forRootAsync({
      useFactory: () => {
        if (databaseUrl) {
          return {
            type: 'postgres',
            url: databaseUrl,
            entities: [NonceEntity],
            synchronize: true,
            ssl: process.env.DB_SSL === 'false' ? false : { rejectUnauthorized: false },
          };
        }

        return {
          type: 'sqlite',
          database: dbPath,
          entities: [NonceEntity],
          synchronize: true,
        };
      },
    }),
    NonceModule,
    BlockchainModule,
    QueueModule,
    TransactionModule,
  ],
})
export class AppModule {}
