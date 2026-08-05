import { Module } from '@nestjs/common';
import { TransactionController } from './transaction.controller';
import { TransactionService } from './transaction.service';
import { QueueModule } from '../queue/queue.module';
import { NonceModule } from '../nonce/nonce.module';
import { BlockchainModule } from '../blockchain/blockchain.module';

@Module({
  imports: [QueueModule, NonceModule, BlockchainModule],
  controllers: [TransactionController],
  providers: [TransactionService],
})
export class TransactionModule {}
