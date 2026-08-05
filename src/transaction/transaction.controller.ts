import { Controller, Post, Get, Body, Param } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiProperty, ApiParam } from '@nestjs/swagger';
import { TransactionService } from './transaction.service';
import { IsString, IsNotEmpty, IsNumber, IsPositive } from 'class-validator';

export class SendTransactionDto {
  @ApiProperty({
    description: 'Recipient EVM wallet address',
    example: '0x70997970C51812dc3A010C7d01b50e0d17dc79C8',
  })
  @IsString()
  @IsNotEmpty()
  toWallet: string;

  @ApiProperty({
    description: 'Amount of tokens to transfer',
    example: 0.001,
  })
  @IsNumber()
  @IsPositive()
  amount: number;
}

@ApiTags('Transactions')
@Controller()
export class TransactionController {
  constructor(private readonly transactionService: TransactionService) {}

  @Get()
  @ApiOperation({
    summary: 'API Root & Health Check',
    description: 'Returns API server status and available endpoints',
    operationId: 'getRootInfo',
  })
  @ApiResponse({ status: 200, description: 'Server is online and healthy' })
  getRootInfo() {
    return {
      message: 'BullMQ + Nonce Management NestJS Backend is running!',
      status: 'ONLINE',
      swaggerUi: 'http://localhost:3000/api',
      availableEndpoints: {
        swaggerDocs: 'GET /api',
        sendTransaction: 'POST /transaction/send',
        getAllTransactions: 'GET /transaction/all',
        getTransactionStatus: 'GET /transaction/:id',
        getQueueStatus: 'GET /queue/status',
      },
    };
  }

  @Post('transaction/send')
  @ApiOperation({
    summary: 'Send Native Token Transfer',
    description: 'Enqueues a real EVM token transfer transaction into BullMQ for asynchronous execution using the configured private key',
    operationId: 'sendTransaction',
  })
  @ApiResponse({
    status: 201,
    description: 'Transaction successfully enqueued',
    schema: {
      example: {
        message: 'Transaction added to queue',
        jobId: '1',
      },
    },
  })
  async sendTransaction(@Body() dto: SendTransactionDto) {
    return this.transactionService.addTransactionToQueue(dto);
  }

  @Get('transaction/all')
  @ApiOperation({
    summary: 'Get All Transactions & Nonces',
    description: 'Retrieves all transaction history, nonces, and transaction hashes from the database',
    operationId: 'getAllTransactions',
  })
  @ApiResponse({ status: 200, description: 'Returns all transaction records' })
  async getAllTransactions() {
    return this.transactionService.getAllTransactions();
  }

  @Get('transaction/:id')
  @ApiOperation({
    summary: 'Get Transaction Job Status',
    description: 'Fetches job processing state, result, and attempts by Job ID',
    operationId: 'getTransactionStatus',
  })
  @ApiParam({ name: 'id', description: 'BullMQ Job ID', example: '1' })
  @ApiResponse({ status: 200, description: 'Returns Job status and output' })
  @ApiResponse({ status: 404, description: 'Job ID not found' })
  async getTransactionStatus(@Param('id') id: string) {
    return this.transactionService.getTransactionStatus(id);
  }

  @Get('queue/status')
  @ApiOperation({
    summary: 'Get Queue Metrics',
    description: 'Retrieves current active, waiting, completed, and failed job metrics from BullMQ queue',
    operationId: 'getQueueStatus',
  })
  @ApiResponse({ status: 200, description: 'Returns queue metrics' })
  async getQueueStatus() {
    return this.transactionService.getQueueStatus();
  }
}
