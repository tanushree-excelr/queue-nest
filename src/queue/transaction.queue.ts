export const TRANSACTION_QUEUE_NAME = 'transaction-queue';

export interface CreateTransactionJobDto {
  toWallet: string;
  amount: number;
  tokenAddress?: string;
}
