// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/governance/Governor.sol";
import "@openzeppelin/contracts/governance/extensions/GovernorSettings.sol";
import "@openzeppelin/contracts/governance/extensions/GovernorCountingSimple.sol";
import "@openzeppelin/contracts/governance/extensions/GovernorVotes.sol";
import "@openzeppelin/contracts/governance/extensions/GovernorVotesQuorumFraction.sol";
import "@openzeppelin/contracts/governance/extensions/GovernorTimelockControl.sol";
import "@openzeppelin/contracts/governance/utils/IVotes.sol";
import "@openzeppelin/contracts/governance/TimelockController.sol";

/**
 * @title VersionGovernance
 * @dev Governance for node version management using OpenZeppelin's Governor framework.
 */
contract VersionGovernance is
    Governor,
    GovernorSettings,
    GovernorCountingSimple,
    GovernorVotes,
    GovernorVotesQuorumFraction,
    GovernorTimelockControl
{
    struct Version {
        uint64 sequence;
        uint256 activatedAt;
        string nePcr0;
        string nePcr1;
        string nePcr2;
        bool active;
    }

    // Version storage
    mapping(uint64 => Version) public versions;

    // Events
    event VersionAdded(uint64 sequence, string nePcr0, string nePcr1, string nePcr2);
    event VersionActivated(uint64 sequence);
    event VersionDeactivated(uint64 sequence);

    /**
     * @dev Constructor to initialize the governor contract.
     * @param _token The token used for governance voting
     * @param _timelock The timelock controller for delayed execution
     * @param _votingDelay The delay before voting starts after a proposal is created (in blocks)
     * @param _votingPeriod The voting period duration (in blocks)
     * @param _quorumFraction The fraction of total supply required for quorum (in percentage)
     * @param _proposalThreshold The minimum amount of votes required to create a proposal
     */
    constructor(
        IVotes _token,
        TimelockController _timelock,
        uint48 _votingDelay,
        uint32 _votingPeriod,
        uint256 _quorumFraction,
        uint256 _proposalThreshold
    )
        Governor("VersionGovernance")
        GovernorSettings(_votingDelay, _votingPeriod, _proposalThreshold)
        GovernorVotes(_token)
        GovernorVotesQuorumFraction(_quorumFraction)
        GovernorTimelockControl(_timelock)
    {}

    /**
     * @dev Add a new version (can only be called through governance).
     */
    function addVersion(
        uint64 sequence,
        string memory nePcr0,
        string memory nePcr1,
        string memory nePcr2
    ) public onlyGovernance {
        require(versions[sequence].sequence == 0, "Version already exists");

        versions[sequence] = Version({
            sequence: sequence,
            activatedAt: block.timestamp,
            nePcr0: nePcr0,
            nePcr1: nePcr1,
            nePcr2: nePcr2,
            active: false
        });

        emit VersionAdded(sequence, nePcr0, nePcr1, nePcr2);
    }

    /**
     * @dev Activate a version (can only be called through governance).
     */
    function activateVersion(uint64 sequence) public onlyGovernance {
        require(versions[sequence].sequence != 0, "Version does not exist");
        require(!versions[sequence].active, "Version already active");

        versions[sequence].active = true;
        versions[sequence].activatedAt = block.timestamp;

        emit VersionActivated(sequence);
    }

    /**
     * @dev Deactivate a version (can only be called through governance).
     */
    function deactivateVersion(uint64 sequence) public onlyGovernance {
        require(versions[sequence].sequence != 0, "Version does not exist");
        require(versions[sequence].active, "Version not active");

        versions[sequence].active = false;

        emit VersionDeactivated(sequence);
    }

    /**
     * @dev Get all active versions.
     */
    function getActiveVersions() external view returns (Version[] memory) {
        uint256 count = 0;

        // Count active versions
        for (uint64 i = 0; i < 1000; i++) { // Arbitrary limit for safety
            if (versions[i].sequence == 0) continue;
            if (versions[i].active) count++;
        }

        // Create result array
        Version[] memory activeVersions = new Version[](count);
        uint256 index = 0;

        // Fill result array
        for (uint64 i = 0; i < 1000; i++) {
            if (versions[i].sequence == 0) continue;
            if (versions[i].active) {
                activeVersions[index] = versions[i];
                index++;
            }
        }

        return activeVersions;
    }

    // The following functions are overrides required by Solidity.

    function proposalThreshold()
        public
        view
        override(Governor, GovernorSettings)
        returns (uint256)
    {
        return super.proposalThreshold();
    }

    function votingDelay()
        public
        view
        override(Governor, GovernorSettings)
        returns (uint256)
    {
        return super.votingDelay();
    }

    function votingPeriod()
        public
        view
        override(Governor, GovernorSettings)
        returns (uint256)
    {
        return super.votingPeriod();
    }

    function quorum(uint256 blockNumber)
        public
        view
        override(Governor, GovernorVotesQuorumFraction)
        returns (uint256)
    {
        return super.quorum(blockNumber);
    }

    function state(uint256 proposalId)
        public
        view
        override(Governor, GovernorTimelockControl)
        returns (ProposalState)
    {
        return super.state(proposalId);
    }

    function propose(
        address[] memory targets,
        uint256[] memory values,
        bytes[] memory calldatas,
        string memory description
    )
        public
        override(Governor)
        returns (uint256)
    {
        return super.propose(targets, values, calldatas, description);
    }

    function proposalNeedsQueuing(uint256 proposalId)
        public
        view
        override(Governor, GovernorTimelockControl)
        returns (bool)
    {
        return super.proposalNeedsQueuing(proposalId);
    }

    function _queueOperations(
        uint256 proposalId,
        address[] memory targets,
        uint256[] memory values,
        bytes[] memory calldatas,
        bytes32 descriptionHash
    )
        internal
        override(Governor, GovernorTimelockControl)
        returns (uint48)
    {
        return super._queueOperations(proposalId, targets, values, calldatas, descriptionHash);
    }

    function _executeOperations(
        uint256 proposalId,
        address[] memory targets,
        uint256[] memory values,
        bytes[] memory calldatas,
        bytes32 descriptionHash
    )
        internal
        override(Governor, GovernorTimelockControl)
    {
        super._executeOperations(proposalId, targets, values, calldatas, descriptionHash);
    }

    function _cancel(
        address[] memory targets,
        uint256[] memory values,
        bytes[] memory calldatas,
        bytes32 descriptionHash
    )
        internal
        override(Governor, GovernorTimelockControl)
        returns (uint256)
    {
        return super._cancel(targets, values, calldatas, descriptionHash);
    }

    function _executor()
        internal
        view
        override(Governor, GovernorTimelockControl)
        returns (address)
    {
        return super._executor();
    }
}
