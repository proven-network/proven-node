import { expect } from 'chai';
import { ethers } from 'hardhat';
import { NodeGovernance, ProvenToken, ProvenTimelock } from '../typechain-types';
import { SignerWithAddress } from '@nomicfoundation/hardhat-ethers/signers';
import { time } from '@nomicfoundation/hardhat-network-helpers';
import { ProposalState } from './helpers';

describe('NodeGovernance', function () {
  let nodeGovernance: NodeGovernance;
  let token: ProvenToken;
  let timelock: ProvenTimelock;
  let owner: SignerWithAddress;
  let user1: SignerWithAddress;
  let user2: SignerWithAddress;

  // Governance parameters
  const votingDelay = 1; // 1 block
  const votingPeriod = 5; // 5 blocks
  const quorumFraction = 10; // 10%
  const proposalThreshold = ethers.parseEther('1000'); // 1000 tokens to create a proposal
  const timelockDelay = 60 * 60 * 24; // 1 day
  const initialSupply = ethers.parseEther('1000000'); // 1 million tokens

  beforeEach(async function () {
    // Get signers
    [owner, user1, user2] = await ethers.getSigners();

    // Deploy token
    const ProvenTokenFactory = await ethers.getContractFactory('ProvenToken');
    token = await ProvenTokenFactory.deploy(initialSupply);

    // Distribute tokens
    await token.transfer(user1.address, ethers.parseEther('10000'));
    await token.transfer(user2.address, ethers.parseEther('20000'));

    // Have users delegate to themselves for voting power
    await token.connect(user1).delegate(user1.address);
    await token.connect(user2).delegate(user2.address);
    await token.connect(owner).delegate(owner.address);

    // Deploy timelock
    const ProvenTimelockFactory = await ethers.getContractFactory('ProvenTimelock');
    timelock = await ProvenTimelockFactory.deploy(
      timelockDelay,
      [], // Proposers will be set later (the governor contract)
      [], // Executors will be set later (anyone can execute)
      owner.address // Admin
    );

    // Deploy governance
    const NodeGovernanceFactory = await ethers.getContractFactory('NodeGovernance');
    nodeGovernance = await NodeGovernanceFactory.deploy(
      token,
      timelock,
      votingDelay,
      votingPeriod,
      quorumFraction,
      proposalThreshold
    );

    // Setup roles
    const proposerRole = await timelock.PROPOSER_ROLE();
    const executorRole = await timelock.EXECUTOR_ROLE();
    const adminRole = await timelock.DEFAULT_ADMIN_ROLE();

    // Setup timelock roles
    await timelock.grantRole(proposerRole, nodeGovernance.target);
    await timelock.grantRole(executorRole, ethers.ZeroAddress); // Anyone can execute
    await timelock.revokeRole(adminRole, owner.address); // Renounce admin role for decentralization
  });

  describe('Deployment', function () {
    it('Should set correct governance parameters', async function () {
      expect(await nodeGovernance.votingDelay()).to.equal(votingDelay);
      expect(await nodeGovernance.votingPeriod()).to.equal(votingPeriod);
      expect(await nodeGovernance.proposalThreshold()).to.equal(proposalThreshold);
      const numerator = await nodeGovernance['quorumNumerator()']();
      expect(numerator).to.equal(quorumFraction);
    });

    it('Should set correct token and timelock', async function () {
      expect(await nodeGovernance.token()).to.equal(token.target);
      expect(await nodeGovernance.timelock()).to.equal(timelock.target);
    });
  });

  describe('Node Opportunity Management', function () {
    const region = 'us-east-1';
    const availabilityZone = 'us-east-1a';
    const specializations = [ethers.encodeBytes32String('compute')];
    let opportunityId: string;

    beforeEach(async function () {
      // Calculate opportunity ID for later verification
      opportunityId = ethers.keccak256(
        ethers.solidityPacked(
          ['string', 'string', 'bytes32[]'],
          [region, availabilityZone, specializations]
        )
      );
    });

    it('Should create a node opportunity via governance', async function () {
      // Prepare the proposal
      const createOpportunityCalldata = nodeGovernance.interface.encodeFunctionData(
        'createOpportunity',
        [region, availabilityZone, specializations]
      );

      const description = 'Create a new node opportunity in us-east-1';
      const descHash = ethers.keccak256(ethers.toUtf8Bytes(description));

      // Delegate more voting power to users to ensure we meet quorum
      const totalSupply = await token.totalSupply();
      const quorumNumerator = await nodeGovernance['quorumNumerator()']();
      const quorumAmount = (totalSupply * BigInt(quorumNumerator)) / 100n;

      // Ensure users have enough voting power by transferring more
      const additionalVotes = ethers.parseEther('100000'); // Add 100,000 tokens
      await token.transfer(user1.address, additionalVotes);
      await token.transfer(user2.address, additionalVotes);

      // Re-delegate to update voting power
      await token.connect(user1).delegate(user1.address);
      await token.connect(user2).delegate(user2.address);

      // Propose
      const proposeTx = await nodeGovernance
        .connect(owner)
        .propose([nodeGovernance.target], [0], [createOpportunityCalldata], description);
      await proposeTx.wait();

      // Get proposal ID
      const proposalId = await nodeGovernance.hashProposal(
        [nodeGovernance.target],
        [0],
        [createOpportunityCalldata],
        descHash
      );

      // Move to active state
      await time.increase(votingDelay + 1);
      await ethers.provider.send('evm_mine', []);

      // Vote
      await nodeGovernance.connect(user1).castVote(proposalId, 1);
      await nodeGovernance.connect(user2).castVote(proposalId, 1);

      // Advance to end of voting period
      await time.increase(votingPeriod + 1);
      await ethers.provider.send('evm_mine', []);

      // Make sure we have enough votes and mine extra blocks
      for (let i = 0; i < 5; i++) {
        await ethers.provider.send('evm_mine', []);
      }

      // Check state is succeeded
      const currentState = Number(await nodeGovernance.state(proposalId));

      if (currentState === ProposalState.Defeated) {
        const votes = await nodeGovernance.proposalVotes(proposalId);
        console.log(`Votes needed for quorum: ${ethers.formatEther(quorumAmount)}`);
        console.log(`For votes: ${ethers.formatEther(votes.forVotes)}`);
        console.log(`Against votes: ${ethers.formatEther(votes.againstVotes)}`);
        console.log(`Abstain votes: ${ethers.formatEther(votes.abstainVotes)}`);
      }

      expect(currentState).to.equal(ProposalState.Succeeded);

      // Queue
      await nodeGovernance
        .connect(owner)
        .queue([nodeGovernance.target], [0], [createOpportunityCalldata], descHash);

      // Wait for timelock
      await time.increase(timelockDelay + 1);
      await ethers.provider.send('evm_mine', []);

      // Execute
      await nodeGovernance
        .connect(owner)
        .execute([nodeGovernance.target], [0], [createOpportunityCalldata], descHash);

      // Check that the opportunity was created
      const opportunity = await nodeGovernance.opportunities(opportunityId);
      expect(opportunity.region).to.equal(region);
      expect(opportunity.availabilityZone).to.equal(availabilityZone);
      expect(opportunity.open).to.be.true;
    });

    it('Should not allow non-governance to create opportunities', async function () {
      // Try to call directly (should fail)
      await expect(
        nodeGovernance.connect(owner).createOpportunity(region, availabilityZone, specializations)
      ).to.be.revertedWithCustomError(nodeGovernance, 'GovernorOnlyExecutor');
    });
  });

  describe('Node Candidate Registration', function () {
    let opportunityId: string;
    const region = 'us-east-1';
    const availabilityZone = 'us-east-1a';
    const specializations = [ethers.encodeBytes32String('compute')];
    const origin = 'node1.proven.network';
    const publicKey = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC...';

    beforeEach(async function () {
      // Calculate opportunity ID
      opportunityId = ethers.keccak256(
        ethers.solidityPacked(
          ['string', 'string', 'bytes32[]'],
          [region, availabilityZone, specializations]
        )
      );

      // Delegate more voting power to users to ensure we meet quorum
      const totalSupply = await token.totalSupply();
      const quorumNumerator = await nodeGovernance['quorumNumerator()']();
      const quorumAmount = (totalSupply * BigInt(quorumNumerator)) / 100n;

      // Ensure users have enough voting power by transferring more
      const additionalVotes = ethers.parseEther('100000'); // Add 100,000 tokens
      await token.transfer(user1.address, additionalVotes);
      await token.transfer(user2.address, additionalVotes);

      // Re-delegate to update voting power
      await token.connect(user1).delegate(user1.address);
      await token.connect(user2).delegate(user2.address);

      // Create an opportunity via governance
      const createOpportunityCalldata = nodeGovernance.interface.encodeFunctionData(
        'createOpportunity',
        [region, availabilityZone, specializations]
      );

      const description = 'Create opportunity';
      const descHash = ethers.keccak256(ethers.toUtf8Bytes(description));

      // Propose
      const proposeTx = await nodeGovernance
        .connect(owner)
        .propose([nodeGovernance.target], [0], [createOpportunityCalldata], description);
      await proposeTx.wait();

      // Get proposal ID
      const proposalId = await nodeGovernance.hashProposal(
        [nodeGovernance.target],
        [0],
        [createOpportunityCalldata],
        descHash
      );

      // Move to active state
      await time.increase(votingDelay + 1);
      await ethers.provider.send('evm_mine', []);

      // Vote
      await nodeGovernance.connect(user1).castVote(proposalId, 1);
      await nodeGovernance.connect(user2).castVote(proposalId, 1);

      // Advance to end of voting period
      await time.increase(votingPeriod + 1);
      await ethers.provider.send('evm_mine', []);

      // Make sure we have enough votes and mine extra blocks
      for (let i = 0; i < 5; i++) {
        await ethers.provider.send('evm_mine', []);
      }

      // Check state is succeeded
      const currentState = Number(await nodeGovernance.state(proposalId));

      if (currentState === ProposalState.Defeated) {
        const votes = await nodeGovernance.proposalVotes(proposalId);
        console.log(`Votes needed for quorum: ${ethers.formatEther(quorumAmount)}`);
        console.log(`For votes: ${ethers.formatEther(votes.forVotes)}`);
        console.log(`Against votes: ${ethers.formatEther(votes.againstVotes)}`);
        console.log(`Abstain votes: ${ethers.formatEther(votes.abstainVotes)}`);
      }

      expect(currentState).to.equal(ProposalState.Succeeded);

      // Queue
      await nodeGovernance
        .connect(owner)
        .queue([nodeGovernance.target], [0], [createOpportunityCalldata], descHash);

      // Wait for timelock
      await time.increase(timelockDelay + 1);
      await ethers.provider.send('evm_mine', []);

      // Execute
      await nodeGovernance
        .connect(owner)
        .execute([nodeGovernance.target], [0], [createOpportunityCalldata], descHash);
    });

    it('Should allow users to register as candidates', async function () {
      // Register user1 as a candidate
      await nodeGovernance.connect(user1).registerAsCandidate(opportunityId, origin, publicKey);

      // Check that the candidate was registered
      const candidate = await nodeGovernance.candidates(opportunityId, user1.address);
      expect(candidate.owner).to.equal(user1.address);
      expect(candidate.origin).to.equal(origin);
      expect(candidate.publicKey).to.equal(publicKey);
    });

    it('Should not allow registration for closed opportunities', async function () {
      // Create a new opportunity
      const nonExistentId = ethers.keccak256(ethers.toUtf8Bytes('does-not-exist'));

      // Try to register for non-existent opportunity
      await expect(
        nodeGovernance.connect(user1).registerAsCandidate(nonExistentId, origin, publicKey)
      ).to.be.revertedWith('Opportunity is closed');
    });

    it('Should approve candidates via governance', async function () {
      // Mint more tokens to owner to avoid running out
      const mintAmount = ethers.parseEther('10000000'); // 10M more tokens
      await token.mint(owner.address, mintAmount);

      // Register user1 as a candidate
      await nodeGovernance.connect(user1).registerAsCandidate(opportunityId, origin, publicKey);

      // Prepare calldata for approving the candidate
      const approveCalldata = nodeGovernance.interface.encodeFunctionData('approveCandidate', [
        opportunityId,
        user1.address,
      ]);

      const description = 'Approve node candidate';
      const descHash = ethers.keccak256(ethers.toUtf8Bytes(description));

      // Delegate more voting power to users to ensure we meet quorum
      const totalSupply = await token.totalSupply();
      const quorumNumerator = await nodeGovernance['quorumNumerator()']();
      const quorumAmount = (totalSupply * BigInt(quorumNumerator)) / 100n;

      // Ensure users have enough voting power
      const additionalVotes = ethers.parseEther('2000000'); // Match the other tests with 2M tokens
      await token.transfer(user1.address, additionalVotes);
      await token.transfer(user2.address, additionalVotes);

      // Re-delegate to update voting power
      await token.connect(user1).delegate(user1.address);
      await token.connect(user2).delegate(user2.address);

      // Propose
      const proposeTx = await nodeGovernance
        .connect(owner)
        .propose([nodeGovernance.target], [0], [approveCalldata], description);
      await proposeTx.wait();

      // Get proposal ID
      const proposalId = await nodeGovernance.hashProposal(
        [nodeGovernance.target],
        [0],
        [approveCalldata],
        descHash
      );

      // Move to active state
      await time.increase(votingDelay + 1);
      await ethers.provider.send('evm_mine', []);

      // Vote
      await nodeGovernance.connect(user1).castVote(proposalId, 1);
      await nodeGovernance.connect(user2).castVote(proposalId, 1);

      // Advance to end of voting period
      await time.increase(votingPeriod + 1);
      await ethers.provider.send('evm_mine', []);

      // Make sure we have enough votes and mine extra blocks
      for (let i = 0; i < 5; i++) {
        await ethers.provider.send('evm_mine', []);
      }

      // Check state is succeeded
      let currentState = Number(await nodeGovernance.state(proposalId));

      // Wait for state to change if needed (up to 10 blocks)
      for (let i = 0; i < 10 && currentState !== ProposalState.Succeeded; i++) {
        await ethers.provider.send('evm_mine', []);
        currentState = Number(await nodeGovernance.state(proposalId));
      }

      if (currentState === ProposalState.Defeated) {
        const votes = await nodeGovernance.proposalVotes(proposalId);
        console.log(`Votes needed for quorum: ${ethers.formatEther(quorumAmount)}`);
        console.log(`For votes: ${ethers.formatEther(votes.forVotes)}`);
        console.log(`Against votes: ${ethers.formatEther(votes.againstVotes)}`);
        console.log(`Abstain votes: ${ethers.formatEther(votes.abstainVotes)}`);
      }

      expect(currentState).to.equal(ProposalState.Succeeded);

      // Queue
      await nodeGovernance
        .connect(owner)
        .queue([nodeGovernance.target], [0], [approveCalldata], descHash);

      // Wait for timelock
      await time.increase(timelockDelay + 1);
      await ethers.provider.send('evm_mine', []);

      // Get the node count before execution
      const nodeCountBefore = await nodeGovernance.nodeCount();

      // Execute
      await nodeGovernance
        .connect(owner)
        .execute([nodeGovernance.target], [0], [approveCalldata], descHash);

      // Check that the node count was incremented
      const nodeCountAfter = await nodeGovernance.nodeCount();
      expect(nodeCountAfter).to.equal(nodeCountBefore + 1n);

      // Also check the opportunity is now closed
      const opportunity = await nodeGovernance.opportunities(opportunityId);
      expect(opportunity.open).to.be.false;
    });

    it('Should not allow registration with IP addresses', async function () {
      // Try to register with IPv4
      await expect(
        nodeGovernance.connect(user1).registerAsCandidate(opportunityId, '192.168.1.1', publicKey)
      ).to.be.revertedWith('Origin must be a domain name, not an IP address');

      // Try to register with IPv6
      await expect(
        nodeGovernance
          .connect(user1)
          .registerAsCandidate(opportunityId, '2001:0db8:85a3:0000:0000:8a2e:0370:7334', publicKey)
      ).to.be.revertedWith('Origin must be a domain name, not an IP address');
    });
  });

  describe('Voting mechanisms', function () {
    it('Should calculate quorum correctly', async function () {
      // Get quorum percentage and total supply
      const quorumPercentage = await nodeGovernance['quorumNumerator()']();
      const totalSupply = await token.totalSupply();
      const expectedQuorum = (totalSupply * quorumPercentage) / 100n;

      // Mine a block to create history
      await ethers.provider.send('evm_mine', []);

      // Get the current block number
      const currentBlock = await ethers.provider.getBlockNumber();

      // Query the quorum using the previous block number (which is now in the past)
      const quorum = await nodeGovernance.quorum(currentBlock - 1);

      expect(quorum).to.equal(expectedQuorum);
    });

    it('Should reject proposals below threshold', async function () {
      // Transfer all tokens from user1 so they're below threshold
      await token.connect(user1).transfer(user2.address, await token.balanceOf(user1.address));

      // Try to create a proposal (should fail)
      const calldata = nodeGovernance.interface.encodeFunctionData('createOpportunity', [
        'us-east-1',
        'us-east-1a',
        [ethers.encodeBytes32String('compute')],
      ]);

      await expect(
        nodeGovernance
          .connect(user1)
          .propose([nodeGovernance.target], [0], [calldata], 'Create a new node opportunity')
      ).to.be.revertedWithCustomError(nodeGovernance, 'GovernorInsufficientProposerVotes');
    });

    it('Should count votes correctly', async function () {
      // Create a proposal
      const calldata = nodeGovernance.interface.encodeFunctionData('createOpportunity', [
        'us-east-1',
        'us-east-1a',
        [ethers.encodeBytes32String('compute')],
      ]);

      const proposalTx = await nodeGovernance
        .connect(owner)
        .propose([nodeGovernance.target], [0], [calldata], 'Create a new node opportunity');

      const receipt = await proposalTx.wait();
      const event = receipt?.logs.find(
        (log) => nodeGovernance.interface.parseLog(log as any)?.name === 'ProposalCreated'
      );
      const parsedEvent = nodeGovernance.interface.parseLog(event as any);
      const proposalId = parsedEvent?.args[0];

      // Advance blocks to voting delay
      await time.increase(1);

      // Cast votes
      await nodeGovernance.connect(user1).castVote(proposalId, 1); // Vote yes (1)
      await nodeGovernance.connect(user2).castVote(proposalId, 0); // Vote no (0)
      await nodeGovernance.connect(owner).castVote(proposalId, 2); // Vote abstain (2)

      // Get proposal votes
      const votes = await nodeGovernance.proposalVotes(proposalId);

      // Check votes are counted correctly
      expect(votes.againstVotes).to.equal(await token.getVotes(user2.address));
      expect(votes.forVotes).to.equal(await token.getVotes(user1.address));
      expect(votes.abstainVotes).to.equal(await token.getVotes(owner.address));
    });
  });
});
