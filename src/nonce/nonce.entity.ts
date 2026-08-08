import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, UpdateDateColumn, Index } from 'typeorm';

export enum NonceStatus {
  PENDING = 'PENDING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

@Entity('nonces')
export class NonceEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column({ nullable: true })
  @Index()
  jobId: string;

  @Column()
  @Index()
  walletAddress: string;

  @Column({ nullable: true })
  toWallet: string;

  @Column({ type: 'float', nullable: true })
  amount: number;

  @Column({ type: 'integer', nullable: true })
  nonce: number;

  @Column({
    type: 'varchar',
    default: NonceStatus.PENDING,
  })
  status: NonceStatus;

  @Column({ nullable: true })
  transactionHash: string;

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}
