// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";
import "@openzeppelin/contracts/token/ERC20/extensions/ERC20Permit.sol";
import "@openzeppelin/contracts/token/ERC20/extensions/ERC20Votes.sol";
import "@openzeppelin/contracts/access/Ownable.sol";
import "@openzeppelin/contracts/utils/Nonces.sol";

/**
 * @title ProvenToken
 * @dev ERC20 token with voting capabilities for governance
 */
contract ProvenToken is ERC20, ERC20Permit, ERC20Votes, Ownable {
    /**
     * @dev Constructor that initializes the token with name, symbol and mints initial supply to deployer
     * @param initialSupply The initial amount of tokens to mint
     */
    constructor(uint256 initialSupply)
        ERC20("Proven Network", "PRVN")
        ERC20Permit("Proven Network")
        Ownable(msg.sender)
    {
        _mint(msg.sender, initialSupply);
    }

    /**
     * @dev Mint new tokens (only callable by owner)
     * @param to The address receiving the tokens
     * @param amount The amount of tokens to mint
     */
    function mint(address to, uint256 amount) public onlyOwner {
        _mint(to, amount);
    }

    // The following functions are overrides required by Solidity.

    function _update(address from, address to, uint256 value)
        internal
        override(ERC20, ERC20Votes)
    {
        super._update(from, to, value);
    }

    function nonces(address owner)
        public
        view
        override(ERC20Permit, Nonces)
        returns (uint256)
    {
        return super.nonces(owner);
    }
}
