import { ethers } from 'hardhat';

async function main() {
  const [deployer] = await ethers.getSigners();
  console.log(`Deploying contracts with the account: ${deployer.address}`);

  const initialSupply = ethers.parseEther('10000000'); // 10 million tokens
  const votingDelay = 1; // 1 block
  const votingPeriod = 50400; // ~1 week (assuming 12 sec blocks)
  const quorumPercentage = 4; // 4% of total token supply
  const proposalThreshold = ethers.parseEther('10000'); // 10k tokens to propose
  const timelockDelay = 86400; // 1 day in seconds

  console.log('Deployment parameters:');
  console.log(`  Initial token supply: ${ethers.formatEther(initialSupply)} PRVN`);
  console.log(`  Voting delay: ${votingDelay} blocks`);
  console.log(`  Voting period: ${votingPeriod} blocks (~7 days)`);
  console.log(`  Quorum: ${quorumPercentage}%`);
  console.log(`  Proposal threshold: ${ethers.formatEther(proposalThreshold)} PRVN`);
  console.log(`  Timelock delay: ${timelockDelay / 86400} day(s)`);

  // Deploy ProvenToken
  console.log('\nDeploying ProvenToken...');
  const ProvenToken = await ethers.getContractFactory('ProvenToken');
  const token = await ProvenToken.deploy(initialSupply);
  await token.waitForDeployment();
  console.log(`ProvenToken deployed to: ${await token.getAddress()}`);

  // Deploy ProvenTimelock
  console.log('\nDeploying ProvenTimelock...');
  const ProvenTimelock = await ethers.getContractFactory('ProvenTimelock');
  const timelock = await ProvenTimelock.deploy(
    timelockDelay,
    [], // proposers - will be set after governor deployment
    [], // executors - will be set after governor deployment
    deployer.address // admin
  );
  await timelock.waitForDeployment();
  console.log(`ProvenTimelock deployed to: ${await timelock.getAddress()}`);

  // Deploy VersionGovernance
  console.log('\nDeploying VersionGovernance...');
  const VersionGovernance = await ethers.getContractFactory('VersionGovernance');
  const versionGovernor = await VersionGovernance.deploy(
    await token.getAddress(),
    await timelock.getAddress(),
    votingDelay,
    votingPeriod,
    quorumPercentage,
    proposalThreshold
  );
  await versionGovernor.waitForDeployment();
  console.log(`VersionGovernance deployed to: ${await versionGovernor.getAddress()}`);

  // Deploy NodeGovernance
  console.log('\nDeploying NodeGovernance...');
  const NodeGovernance = await ethers.getContractFactory('NodeGovernance');
  const nodeGovernor = await NodeGovernance.deploy(
    await token.getAddress(),
    await timelock.getAddress(),
    votingDelay,
    votingPeriod,
    quorumPercentage,
    proposalThreshold
  );
  await nodeGovernor.waitForDeployment();
  console.log(`NodeGovernance deployed to: ${await nodeGovernor.getAddress()}`);

  // Configure timelock roles
  console.log('\nConfiguring timelock roles...');
  const timelockContract = await ethers.getContractAt(
    'ProvenTimelock',
    await timelock.getAddress()
  );
  const proposerRole = await timelockContract.PROPOSER_ROLE();
  const executorRole = await timelockContract.EXECUTOR_ROLE();
  const adminRole = await timelockContract.DEFAULT_ADMIN_ROLE();

  // Grant proposer role to governors
  console.log('- Granting proposer role to governance contracts');
  await timelockContract.grantRole(proposerRole, await versionGovernor.getAddress());
  await timelockContract.grantRole(proposerRole, await nodeGovernor.getAddress());

  // Grant executor role to everybody (address zero)
  console.log('- Granting executor role to anyone (address zero)');
  await timelockContract.grantRole(executorRole, ethers.ZeroAddress);

  // Renounce admin role
  console.log('- Revoking admin role from deployer');
  await timelockContract.renounceRole(adminRole, deployer.address);

  console.log('\n✅ Governance system deployment complete!');
  console.log('\nContract addresses:');
  console.log(`ProvenToken: ${await token.getAddress()}`);
  console.log(`ProvenTimelock: ${await timelock.getAddress()}`);
  console.log(`VersionGovernance: ${await versionGovernor.getAddress()}`);
  console.log(`NodeGovernance: ${await nodeGovernor.getAddress()}`);

  console.log('\nNext steps:');
  console.log('1. Distribute ProvenToken for governance participation');
  console.log('2. Token holders should delegate their voting power (token.delegate())');
  console.log('3. Create proposals through the governance contracts');
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
