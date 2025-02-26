// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title TestTarget
 * @dev Simple contract for testing timelock functionality
 */
contract TestTarget {
    uint256 public value;

    function setValue(uint256 newValue) external {
        value = newValue;
    }
}
