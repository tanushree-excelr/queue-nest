import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, UpdateDateColumn, Index } from 'typeorm';

export enum NonceStatus {
  PENDING = 'PENDING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
}

@Entity('nonces')
@Index(['walletAddress', 'nonce'], { unique: true })
export class NonceEntity {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  @Index()
  walletAddress: string;

  @Column({ type: 'integer' })
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
