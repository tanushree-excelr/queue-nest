import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { NonceEntity } from './nonce.entity';
import { NonceService } from './nonce.service';

@Module({
  imports: [TypeOrmModule.forFeature([NonceEntity])],
  providers: [NonceService],
  exports: [NonceService],
})
export class NonceModule {}
