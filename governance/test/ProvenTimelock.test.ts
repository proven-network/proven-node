import { expect } from "chai";
import { ethers } from "hardhat";
import { ProvenTimelock } from "../typechain-types";
import { SignerWithAddress } from "@nomicfoundation/hardhat-ethers/signers";
import { time } from "@nomicfoundation/hardhat-network-helpers";

describe("ProvenTimelock", function () {
  let timelock: ProvenTimelock;
  let owner: SignerWithAddress;
  let proposer: SignerWithAddress;
  let executor: SignerWithAddress;
  let user: SignerWithAddress;
  const minDelay = 60 * 60 * 24; // 1 day in seconds

  beforeEach(async function () {
    // Get signers
    [owner, proposer, executor, user] = await ethers.getSigners();

    // Deploy timelock
    const ProvenTimelockFactory =
      await ethers.getContractFactory("ProvenTimelock");
    const proposers = [proposer.address];
    const executors = [executor.address];

    timelock = await ProvenTimelockFactory.deploy(
      minDelay,
      proposers,
      executors,
      owner.address, // Admin
    );
  });

  describe("Deployment", function () {
    it("Should set the correct minimum delay", async function () {
      expect(await timelock.getMinDelay()).to.equal(minDelay);
    });

    it("Should assign the correct roles", async function () {
      // Check proposer role
      const PROPOSER_ROLE = await timelock.PROPOSER_ROLE();
      expect(await timelock.hasRole(PROPOSER_ROLE, proposer.address)).to.be
        .true;

      // Check executor role
      const EXECUTOR_ROLE = await timelock.EXECUTOR_ROLE();
      expect(await timelock.hasRole(EXECUTOR_ROLE, executor.address)).to.be
        .true;

      // Check admin role
      const TIMELOCK_ADMIN_ROLE = await timelock.DEFAULT_ADMIN_ROLE();
      expect(await timelock.hasRole(TIMELOCK_ADMIN_ROLE, owner.address)).to.be
        .true;
    });
  });

  describe("Operation scheduling and execution", function () {
    let mockContract: any;
    let callData: string;
    const salt = ethers.hexlify(ethers.randomBytes(32));
    const value = 0;

    beforeEach(async function () {
      // Deploy a mock contract that we can call through the timelock
      const MockContract = await ethers.getContractFactory("TestTarget");
      mockContract = await MockContract.deploy();

      // Prepare calldata for a function call
      callData = mockContract.interface.encodeFunctionData("setValue", [42]);
    });

    it("Should allow a proposer to schedule an operation", async function () {
      // Schedule an operation
      await timelock.connect(proposer).schedule(
        mockContract.target,
        value,
        callData,
        ethers.ZeroHash, // Predecessor (none in this case)
        salt,
        minDelay,
      );

      // Check that the operation is pending
      const operationId = await timelock.hashOperation(
        mockContract.target,
        value,
        callData,
        ethers.ZeroHash,
        salt,
      );

      expect(await timelock.isOperationPending(operationId)).to.be.true;
      expect(await timelock.isOperationReady(operationId)).to.be.false;
      expect(await timelock.isOperationDone(operationId)).to.be.false;
    });

    it("Should prevent non-proposers from scheduling operations", async function () {
      // Try to schedule an operation as a non-proposer
      await expect(
        timelock
          .connect(user)
          .schedule(
            mockContract.target,
            value,
            callData,
            ethers.ZeroHash,
            salt,
            minDelay,
          ),
      ).to.be.revertedWithCustomError(
        timelock,
        "AccessControlUnauthorizedAccount",
      );
    });

    it("Should allow execution only after the time delay", async function () {
      // Schedule an operation
      await timelock
        .connect(proposer)
        .schedule(
          mockContract.target,
          value,
          callData,
          ethers.ZeroHash,
          salt,
          minDelay,
        );

      const operationId = await timelock.hashOperation(
        mockContract.target,
        value,
        callData,
        ethers.ZeroHash,
        salt,
      );

      // Try to execute immediately (should fail)
      await expect(
        timelock
          .connect(executor)
          .execute(mockContract.target, value, callData, ethers.ZeroHash, salt),
      ).to.be.revertedWithCustomError(
        timelock,
        "TimelockUnexpectedOperationState",
      );

      // Move time forward past the delay
      await time.increase(minDelay + 1);

      // Check that the operation is ready
      expect(await timelock.isOperationReady(operationId)).to.be.true;

      // Now execute the operation
      await timelock
        .connect(executor)
        .execute(mockContract.target, value, callData, ethers.ZeroHash, salt);

      // Check that the operation is done
      expect(await timelock.isOperationDone(operationId)).to.be.true;

      // Check that the target contract state was updated
      expect(await mockContract.value()).to.equal(42);
    });

    it("Should prevent non-executors from executing operations", async function () {
      // Schedule an operation
      await timelock
        .connect(proposer)
        .schedule(
          mockContract.target,
          value,
          callData,
          ethers.ZeroHash,
          salt,
          minDelay,
        );

      // Move time forward past the delay
      await time.increase(minDelay + 1);

      // Try to execute as a non-executor
      await expect(
        timelock
          .connect(user)
          .execute(mockContract.target, value, callData, ethers.ZeroHash, salt),
      ).to.be.revertedWithCustomError(
        timelock,
        "AccessControlUnauthorizedAccount",
      );
    });

    it("Should allow cancellation of scheduled operations", async function () {
      // Schedule an operation
      await timelock
        .connect(proposer)
        .schedule(
          mockContract.target,
          value,
          callData,
          ethers.ZeroHash,
          salt,
          minDelay,
        );

      const operationId = await timelock.hashOperation(
        mockContract.target,
        value,
        callData,
        ethers.ZeroHash,
        salt,
      );

      // Cancel the operation
      await timelock.connect(proposer).cancel(operationId);

      // Check that the operation is no longer pending
      expect(await timelock.isOperationPending(operationId)).to.be.false;

      // Move time forward past the delay
      await time.increase(minDelay + 1);

      // Try to execute the cancelled operation (should fail)
      await expect(
        timelock
          .connect(executor)
          .execute(mockContract.target, value, callData, ethers.ZeroHash, salt),
      ).to.be.revertedWithCustomError(
        timelock,
        "TimelockUnexpectedOperationState",
      );
    });
  });

  describe("Role management", function () {
    it("Should allow admin to grant roles", async function () {
      const newProposer = user.address;
      const PROPOSER_ROLE = await timelock.PROPOSER_ROLE();

      // Grant proposer role to a new address
      await timelock.connect(owner).grantRole(PROPOSER_ROLE, newProposer);

      // Check that the role was granted
      expect(await timelock.hasRole(PROPOSER_ROLE, newProposer)).to.be.true;
    });

    it("Should prevent non-admins from granting roles", async function () {
      const newProposer = user.address;
      const PROPOSER_ROLE = await timelock.PROPOSER_ROLE();

      // Try to grant proposer role as a non-admin
      await expect(
        timelock.connect(proposer).grantRole(PROPOSER_ROLE, newProposer),
      ).to.be.revertedWithCustomError(
        timelock,
        "AccessControlUnauthorizedAccount",
      );
    });

    it("Should allow admin to revoke roles", async function () {
      const PROPOSER_ROLE = await timelock.PROPOSER_ROLE();

      // Revoke proposer role
      await timelock.connect(owner).revokeRole(PROPOSER_ROLE, proposer.address);

      // Check that the role was revoked
      expect(await timelock.hasRole(PROPOSER_ROLE, proposer.address)).to.be
        .false;
    });
  });
});
