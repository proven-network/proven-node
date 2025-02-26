// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/governance/TimelockController.sol";

/**
 * @title ProvenTimelock
 * @dev Timelock controller for delayed execution of governance proposals
 */
contract ProvenTimelock is TimelockController {
    /**
     * @dev Constructor that sets up the timelock with minimum delay and appropriate roles
     * @param minDelay The minimum delay before execution
     * @param proposers The addresses that can propose delayed transactions
     * @param executors The addresses that can execute delayed transactions
     * @param admin The address that can manage the timelock (can be address(0) for no admin)
     */
    constructor(
        uint256 minDelay,
        address[] memory proposers,
        address[] memory executors,
        address admin
    ) TimelockController(minDelay, proposers, executors, admin) {}
}
