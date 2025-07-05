import { expect } from 'chai';
import { ethers } from 'hardhat';
import { ProvenToken } from '../typechain-types';
import { SignerWithAddress } from '@nomicfoundation/hardhat-ethers/signers';

describe('ProvenToken', function () {
  let provenToken: ProvenToken;
  let owner: SignerWithAddress;
  let user1: SignerWithAddress;
  let user2: SignerWithAddress;
  const initialSupply = ethers.parseEther('1000000'); // 1 million tokens

  beforeEach(async function () {
    // Get signers
    [owner, user1, user2] = await ethers.getSigners();

    // Deploy token
    const ProvenTokenFactory = await ethers.getContractFactory('ProvenToken');
    provenToken = await ProvenTokenFactory.deploy(initialSupply);
  });

  describe('Deployment', function () {
    it('Should set the right owner', async function () {
      expect(await provenToken.owner()).to.equal(owner.address);
    });

    it('Should assign the total supply of tokens to the owner', async function () {
      const ownerBalance = await provenToken.balanceOf(owner.address);
      expect(await provenToken.totalSupply()).to.equal(ownerBalance);
    });

    it('Should set correct token name and symbol', async function () {
      expect(await provenToken.name()).to.equal('Proven Network');
      expect(await provenToken.symbol()).to.equal('PRVN');
    });
  });

  describe('Transactions', function () {
    it('Should transfer tokens between accounts', async function () {
      // Transfer 50 tokens from owner to user1
      await provenToken.transfer(user1.address, ethers.parseEther('50'));
      const user1Balance = await provenToken.balanceOf(user1.address);
      expect(user1Balance).to.equal(ethers.parseEther('50'));

      // Transfer 20 tokens from user1 to user2
      await provenToken.connect(user1).transfer(user2.address, ethers.parseEther('20'));
      const user2Balance = await provenToken.balanceOf(user2.address);
      expect(user2Balance).to.equal(ethers.parseEther('20'));
    });

    it("Should fail if sender doesn't have enough tokens", async function () {
      const initialOwnerBalance = await provenToken.balanceOf(owner.address);

      // Try to send more tokens than the owner has
      await expect(
        provenToken.connect(user1).transfer(owner.address, ethers.parseEther('1'))
      ).to.be.revertedWithCustomError(provenToken, 'ERC20InsufficientBalance');

      // Owner balance shouldn't have changed
      expect(await provenToken.balanceOf(owner.address)).to.equal(initialOwnerBalance);
    });

    it('Should update balances after transfers', async function () {
      const initialOwnerBalance = await provenToken.balanceOf(owner.address);

      // Transfer 100 tokens from owner to user1
      await provenToken.transfer(user1.address, ethers.parseEther('100'));

      // Transfer 50 tokens from owner to user2
      await provenToken.transfer(user2.address, ethers.parseEther('50'));

      // Check balances
      const finalOwnerBalance = await provenToken.balanceOf(owner.address);
      expect(finalOwnerBalance).to.equal(initialOwnerBalance - ethers.parseEther('150'));

      const user1Balance = await provenToken.balanceOf(user1.address);
      expect(user1Balance).to.equal(ethers.parseEther('100'));

      const user2Balance = await provenToken.balanceOf(user2.address);
      expect(user2Balance).to.equal(ethers.parseEther('50'));
    });
  });

  describe('Minting', function () {
    it('Should allow owner to mint tokens', async function () {
      const initialSupply = await provenToken.totalSupply();

      // Mint 1000 more tokens
      await provenToken.mint(user1.address, ethers.parseEther('1000'));

      // Check updated total supply
      expect(await provenToken.totalSupply()).to.equal(initialSupply + ethers.parseEther('1000'));

      // Check user1 received the minted tokens
      expect(await provenToken.balanceOf(user1.address)).to.equal(ethers.parseEther('1000'));
    });

    it('Should prevent non-owners from minting tokens', async function () {
      await expect(
        provenToken.connect(user1).mint(user1.address, ethers.parseEther('1000'))
      ).to.be.revertedWithCustomError(provenToken, 'OwnableUnauthorizedAccount');
    });
  });

  describe('Voting power', function () {
    it('Should track voting power when tokens are transferred', async function () {
      // Transfer tokens to user1
      await provenToken.transfer(user1.address, ethers.parseEther('100'));

      // Check voting power
      expect(await provenToken.getVotes(user1.address)).to.equal(0); // No votes yet until delegation

      // User1 delegates to self
      await provenToken.connect(user1).delegate(user1.address);

      // Check updated voting power
      expect(await provenToken.getVotes(user1.address)).to.equal(ethers.parseEther('100'));

      // Transfer more tokens to user1
      await provenToken.transfer(user1.address, ethers.parseEther('50'));

      // Check updated voting power
      expect(await provenToken.getVotes(user1.address)).to.equal(ethers.parseEther('150'));
    });

    it('Should allow delegating voting power', async function () {
      // Owner delegates to user1
      await provenToken.delegate(user1.address);

      // Check voting power
      expect(await provenToken.getVotes(user1.address)).to.equal(initialSupply);
      expect(await provenToken.getVotes(owner.address)).to.equal(0);

      // User1 doesn't have tokens to delegate, so no effect on voting power
      await provenToken.connect(user1).delegate(user2.address);

      // User1 still has the voting power delegated from owner
      expect(await provenToken.getVotes(user1.address)).to.equal(initialSupply);
      expect(await provenToken.getVotes(user2.address)).to.equal(0);
    });
  });
});
