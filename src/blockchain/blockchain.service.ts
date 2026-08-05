import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { JsonRpcProvider, Wallet, parseEther, parseUnits, isAddress, Contract } from 'ethers';

export interface TransactionResult {
  transactionHash: string;
  nonce: number;
  status: 'SUCCESS' | 'FAILED';
  fromWallet: string;
  toWallet: string;
  amount: number;
  tokenAddress?: string;
  timestamp: string;
}

const ERC20_ABI = [
  'function transfer(address to, uint256 amount) returns (bool)',
  'function decimals() view returns (uint8)',
  'function balanceOf(address owner) view returns (uint256)',
];

@Injectable()
export class BlockchainService implements OnModuleInit {
  private readonly logger = new Logger(BlockchainService.name);
  private provider: JsonRpcProvider;
  private wallet: Wallet;

  onModuleInit() {
    const rpcUrl = process.env.RPC_URL || 'https://testnetrpc.mstblockchain.com/';
    const privateKey = process.env.PRIVATE_KEY;

    this.provider = new JsonRpcProvider(rpcUrl);

    if (privateKey) {
      try {
        const formattedKey = privateKey.startsWith('0x') ? privateKey : `0x${privateKey}`;
        this.wallet = new Wallet(formattedKey, this.provider);
        this.logger.log(`[Blockchain Service] Connected to RPC: ${rpcUrl}`);
        this.logger.log(`[Blockchain Service] Sender Wallet Address derived: ${this.wallet.address}`);
      } catch (err) {
        this.logger.error(`[Blockchain Service] Invalid PRIVATE_KEY in .env: ${err.message}`);
      }
    } else {
      this.logger.warn(`[Blockchain Service] PRIVATE_KEY not found in .env. Please configure PRIVATE_KEY.`);
    }
  }

  getWalletAddress(): string {
    if (!this.wallet) {
      throw new Error('Sender wallet is not initialized. Check PRIVATE_KEY in .env');
    }
    return this.wallet.address;
  }

  async getNetworkNonce(address: string): Promise<number> {
    try {
      const count = await this.provider.getTransactionCount(address, 'pending');
      this.logger.log(`[Blockchain Service] Live network pending nonce for ${address}: ${count}`);
      return count;
    } catch (error) {
      this.logger.warn(`[Blockchain Service] Could not fetch network nonce: ${error.message}. Defaulting to 0.`);
      return 0;
    }
  }

  async sendTransaction(
    toWallet: string,
    amount: number,
    nonce: number,
    tokenAddress?: string,
  ): Promise<TransactionResult> {
    if (!isAddress(toWallet)) {
      throw new Error(`Invalid recipient EVM wallet address: ${toWallet}`);
    }

    const fromAddress = this.getWalletAddress();

    try {
      let txResponse;

      if (tokenAddress) {
        if (!isAddress(tokenAddress)) {
          throw new Error(`Invalid ERC-20 token contract address: ${tokenAddress}`);
        }

        this.logger.log(
          `[Blockchain Broadcast] Sending ${amount} ERC-20 tokens (Contract: ${tokenAddress}) from ${fromAddress} to ${toWallet} with Nonce: ${nonce}`,
        );

        const tokenContract = new Contract(tokenAddress, ERC20_ABI, this.wallet);
        const decimals = await tokenContract.decimals().catch(() => 18);
        const parsedAmount = parseUnits(amount.toString(), decimals);

        const feeData: any = await this.provider.getFeeData().catch(() => ({}));
        const txOverrides: any = { nonce };
        if (feeData && feeData.gasPrice) txOverrides.gasPrice = feeData.gasPrice;

        txResponse = await tokenContract.transfer(toWallet, parsedAmount, txOverrides);
      } else {
        this.logger.log(
          `[Blockchain Broadcast] Sending ${amount} Native tokens from ${fromAddress} to ${toWallet} with Nonce: ${nonce}`,
        );

        const feeData: any = await this.provider.getFeeData().catch(() => ({}));
        const txPayload: any = {
          to: toWallet,
          value: parseEther(amount.toString()),
          nonce: nonce,
        };

        if (feeData && feeData.gasPrice) txPayload.gasPrice = feeData.gasPrice;

        txResponse = await this.wallet.sendTransaction(txPayload);
      }

      this.logger.log(
        `[Blockchain Broadcast Success] Transaction broadcasted! Tx Hash: ${txResponse.hash} | Reserved Nonce: ${nonce}`,
      );

      return {
        transactionHash: txResponse.hash,
        nonce,
        status: 'SUCCESS',
        fromWallet: fromAddress,
        toWallet,
        amount,
        tokenAddress,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      this.logger.error(
        `[Blockchain Broadcast Failed] Error sending transaction with nonce ${nonce}: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }
}
