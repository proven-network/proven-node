import { ethers } from "hardhat";
import { time } from "@nomicfoundation/hardhat-toolbox/network-helpers";
import { expect } from "chai";
import { Contract, Signer } from "ethers";
import { HardhatEthersSigner } from "@nomicfoundation/hardhat-ethers/signers";
import { NodeGovernance, VersionGovernance } from "../typechain-types";

export const VOTING_DELAY = 1n; // 1 block
export const VOTING_PERIOD = 50400n; // 1 week assuming 12 sec blocks
export const QUORUM_FRACTION = 4n; // 4%
export const PROPOSAL_THRESHOLD = ethers.parseEther("10000"); // 10,000 tokens
export const TIMELOCK_DELAY = 86400n; // 1 day
export const ZERO_ADDRESS = ethers.ZeroAddress;
export const INITIAL_SUPPLY = ethers.parseEther("10000000"); // 10M tokens

// OpenZeppelin Governor ProposalState enum
export enum ProposalState {
  Pending,
  Active,
  Canceled,
  Defeated,
  Succeeded,
  Queued,
  Expired,
  Executed,
}

// Create a proposal description string from an action description
export function proposalDescription(action: string): string {
  return `Proposal #1: ${action}`;
}

// Calculate proposal id
export async function getProposalId(
  governor: VersionGovernance | NodeGovernance,
  targets: string[],
  values: bigint[],
  calldatas: string[],
  description: string
): Promise<bigint> {
  const descHash = ethers.id(description);
  return await governor.hashProposal(targets, values, calldatas, descHash);
}

// Helper for moving through proposal lifecycle
export async function advanceToState(
  governor: VersionGovernance | NodeGovernance,
  proposalId: bigint,
  targetState: ProposalState,
  description: string,
  targets: string[] = [],
  values: bigint[] = [],
  calldatas: string[] = []
): Promise<void> {
  let state = Number(await governor.state(proposalId));
  const descHash = ethers.id(description);

  // Move from Pending to Active
  if (state === ProposalState.Pending && targetState >= ProposalState.Active) {
    await time.increase(VOTING_DELAY + 1n);
    await ethers.provider.send("evm_mine", []);
    state = Number(await governor.state(proposalId));
  }

  // Wait for voting period to end and move to Succeeded
  if (
    state === ProposalState.Active &&
    targetState >= ProposalState.Succeeded
  ) {
    await time.increase(VOTING_PERIOD + 1n);
    await ethers.provider.send("evm_mine", []);

    // Mine a few blocks to ensure the vote is counted correctly
    for (let i = 0; i < 5; i++) {
      await ethers.provider.send("evm_mine", []);
    }

    state = Number(await governor.state(proposalId));

    // Make sure we succeeded and didn't defeat
    if (state === ProposalState.Defeated) {
      throw new Error(
        `Proposal was defeated. Check the voting power/quorum requirements.`
      );
    }
  }

  // Move from Succeeded to Queued
  if (
    state === ProposalState.Succeeded &&
    targetState >= ProposalState.Queued
  ) {
    await governor.queue(targets, values, calldatas, descHash);
    state = Number(await governor.state(proposalId));
  }

  // Move from Queued to Executed
  if (state === ProposalState.Queued && targetState >= ProposalState.Executed) {
    await time.increase(TIMELOCK_DELAY + 1n);
    await ethers.provider.send("evm_mine", []);
    await governor.execute(targets, values, calldatas, descHash);
    state = Number(await governor.state(proposalId));
  }

  expect(Number(await governor.state(proposalId))).to.equal(targetState);
}

// Deploy contracts for testing
export async function deployContracts() {
  const [deployer, voter1, voter2, voter3, voter4] = await ethers.getSigners();

  // Deploy token
  const ProvenToken = await ethers.getContractFactory("ProvenToken");
  const token = await ProvenToken.deploy(INITIAL_SUPPLY);

  // Deploy timelock with proposers and executors to be set later
  const ProvenTimelock = await ethers.getContractFactory("ProvenTimelock");
  const timelock = await ProvenTimelock.deploy(
    TIMELOCK_DELAY,
    [], // proposers
    [], // executors
    deployer.address
  );

  // Deploy VersionGovernance
  const VersionGovernance = await ethers.getContractFactory(
    "VersionGovernance"
  );
  const versionGovernor = await VersionGovernance.deploy(
    await token.getAddress(),
    await timelock.getAddress(),
    VOTING_DELAY,
    VOTING_PERIOD,
    QUORUM_FRACTION,
    PROPOSAL_THRESHOLD
  );

  // Deploy NodeGovernance
  const NodeGovernance = await ethers.getContractFactory("NodeGovernance");
  const nodeGovernor = await NodeGovernance.deploy(
    await token.getAddress(),
    await timelock.getAddress(),
    VOTING_DELAY,
    VOTING_PERIOD,
    QUORUM_FRACTION,
    PROPOSAL_THRESHOLD
  );

  // Setup roles
  const proposerRole = await timelock.PROPOSER_ROLE();
  const executorRole = await timelock.EXECUTOR_ROLE();
  const adminRole = await timelock.DEFAULT_ADMIN_ROLE();

  // Grant proposer role to governors
  await timelock.grantRole(proposerRole, await versionGovernor.getAddress());
  await timelock.grantRole(proposerRole, await nodeGovernor.getAddress());

  // Grant executor role to zero address (anyone can execute)
  await timelock.grantRole(executorRole, ZERO_ADDRESS);

  // Renounce admin role (no admin can change timelock)
  await timelock.renounceRole(adminRole, deployer.address);

  // Distribute tokens for voting
  const voters = [voter1, voter2, voter3, voter4];
  for (const voter of voters) {
    const amount = ethers.parseEther("1000000"); // 1M tokens
    await token.transfer(voter.address, amount);
    await token.connect(voter).delegate(voter.address);
  }

  return {
    token,
    timelock,
    versionGovernor,
    nodeGovernor,
    deployer,
    voters,
    voter1,
    voter2,
    voter3,
  };
}

// Create a proposal that will pass
export async function createPassingProposal(
  governor: NodeGovernance | VersionGovernance,
  token: Contract,
  targets: string[],
  values: bigint[],
  calldatas: string[],
  description: string,
  voters: HardhatEthersSigner[]
) {
  // Submit proposal
  const proposeTx = await governor.propose(
    targets,
    values,
    calldatas,
    description
  );
  await proposeTx.wait();

  // Get proposal id from events
  const proposalId = await getProposalId(
    governor,
    targets,
    values,
    calldatas,
    description
  );

  // Move to active state
  await advanceToState(
    governor,
    proposalId,
    ProposalState.Active,
    description,
    targets,
    values,
    calldatas
  );

  // Cast votes from multiple voters
  for (const voter of voters) {
    await governor.connect(voter).castVote(proposalId, 1); // Vote in favor
  }

  // Wait for voting period to end and queue the proposal
  await advanceToState(
    governor,
    proposalId,
    ProposalState.Queued,
    description,
    targets,
    values,
    calldatas
  );

  // Wait for timelock delay and execute
  await advanceToState(
    governor,
    proposalId,
    ProposalState.Executed,
    description,
    targets,
    values,
    calldatas
  );

  return { proposalId, description };
}
