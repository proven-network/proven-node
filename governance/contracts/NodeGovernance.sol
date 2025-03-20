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
 * @title NodeGovernance
 * @dev Governance for network topology management using OpenZeppelin's Governor framework.
 */
contract NodeGovernance is
    Governor,
    GovernorSettings,
    GovernorCountingSimple,
    GovernorVotes,
    GovernorVotesQuorumFraction,
    GovernorTimelockControl
{
    struct NodeOpportunity {
        string region;
        string availabilityZone;
        bytes32[] specializations;
        bool open;
    }

    struct NodeCandidate {
        address owner;
        string origin;
        string publicKey;
        uint256 votes;
    }

    struct Node {
        string region;
        string availabilityZone;
        string origin;
        string publicKey;
        bytes32[] specializations;
        address owner;
    }

    // Storage
    mapping(bytes32 => NodeOpportunity) public opportunities;
    mapping(bytes32 => mapping(address => NodeCandidate)) public candidates;
    mapping(string => Node) public nodes; // publicKey to Node mapping
    mapping(uint256 => string) public nodePublicKeys; // Index to publicKey mapping for iteration
    uint256 public nodeCount = 0;
    mapping(string => bool) public usedPublicKeys; // Track used public keys

    // Events
    event OpportunityCreated(bytes32 id, string region, string availabilityZone);
    event CandidateRegistered(bytes32 opportunityId, address candidate, string origin, string publicKey);
    event NodeApproved(bytes32 opportunityId, address candidate, string publicKey);
    event NodeRemoved(string publicKey);

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
        Governor("NodeGovernance")
        GovernorSettings(_votingDelay, _votingPeriod, _proposalThreshold)
        GovernorVotes(_token)
        GovernorVotesQuorumFraction(_quorumFraction)
        GovernorTimelockControl(_timelock)
    {}

    /**
     * @dev Create a new node opportunity (can only be called through governance).
     */
    function createOpportunity(
        string memory region,
        string memory availabilityZone,
        bytes32[] memory specializations
    ) public onlyGovernance {
        bytes32 id = keccak256(
            abi.encodePacked(region, availabilityZone, specializations)
        );

        require(!opportunities[id].open, "Opportunity already exists");

        opportunities[id] = NodeOpportunity({
            region: region,
            availabilityZone: availabilityZone,
            specializations: specializations,
            open: true
        });

        emit OpportunityCreated(id, region, availabilityZone);
    }

    /**
     * @dev Check if a string is an IP address (IPv4 or IPv6)
     * @param str The string to check
     * @return True if the string is an IP address, false otherwise
     */
    function isIpAddress(string memory str) internal pure returns (bool) {
        bytes memory strBytes = bytes(str);
        
        // Check for empty string
        if (strBytes.length == 0) {
            return false;
        }

        // Quick check for IPv4 (four groups of 1-3 digits separated by dots)
        bool couldBeIPv4 = true;
        uint8 dotCount = 0;
        uint8 digitCount = 0;
        
        // Quick check for IPv6 (groups separated by colons)
        bool couldBeIPv6 = false;
        uint8 colonCount = 0;
        
        for (uint i = 0; i < strBytes.length; i++) {
            // Check for IPv4 pattern
            if (strBytes[i] >= bytes1('0') && strBytes[i] <= bytes1('9')) {
                if (couldBeIPv4) {
                    digitCount++;
                    // More than 3 digits in a group means not IPv4
                    if (digitCount > 3) {
                        couldBeIPv4 = false;
                    }
                }
            } else if (strBytes[i] == bytes1('.')) {
                if (couldBeIPv4) {
                    // Reset digit count for next group
                    digitCount = 0;
                    dotCount++;
                }
            } else if (strBytes[i] == bytes1(':')) {
                colonCount++;
                couldBeIPv6 = true;
            } else if ((strBytes[i] >= bytes1('a') && strBytes[i] <= bytes1('f')) || 
                      (strBytes[i] >= bytes1('A') && strBytes[i] <= bytes1('F'))) {
                // Hex digits are valid in IPv6
                if (!couldBeIPv6) {
                    couldBeIPv6 = true;
                }
            } else {
                // Any other character means it's not a pure IP address
                if (strBytes[i] == bytes1('-')) {
                    // Domain names can contain hyphens
                    couldBeIPv4 = false;
                    couldBeIPv6 = false;
                }
            }
        }
        
        // Valid IPv4 has exactly 3 dots
        bool isIPv4 = couldBeIPv4 && dotCount == 3;
        
        // Minimum valid IPv6 has at least 2 colons
        bool isIPv6 = couldBeIPv6 && colonCount >= 2;
        
        return isIPv4 || isIPv6;
    }

    /**
     * @dev Register as a candidate for a node opportunity.
     * This is allowed for anyone, not just governance, as individuals need to apply.
     */
    function registerAsCandidate(
        bytes32 opportunityId,
        string memory origin,
        string memory publicKey
    ) external {
        require(opportunities[opportunityId].open, "Opportunity is closed");
        require(!usedPublicKeys[publicKey], "Public key already in use");
        require(!isIpAddress(origin), "Origin must be a domain name, not an IP address");

        candidates[opportunityId][msg.sender] = NodeCandidate({
            owner: msg.sender,
            origin: origin,
            publicKey: publicKey,
            votes: 0
        });

        emit CandidateRegistered(opportunityId, msg.sender, origin, publicKey);
    }

    /**
     * @dev Approve a node candidate (can only be called through governance).
     */
    function approveCandidate(bytes32 opportunityId, address candidate) public onlyGovernance {
        require(candidates[opportunityId][candidate].owner != address(0), "Candidate does not exist");
        require(opportunities[opportunityId].open, "Opportunity is closed");

        NodeCandidate memory winner = candidates[opportunityId][candidate];
        NodeOpportunity memory opportunity = opportunities[opportunityId];
        
        require(!usedPublicKeys[winner.publicKey], "Public key already in use");
        usedPublicKeys[winner.publicKey] = true;

        nodes[winner.publicKey] = Node({
            region: opportunity.region,
            availabilityZone: opportunity.availabilityZone,
            origin: winner.origin,
            publicKey: winner.publicKey,
            specializations: opportunity.specializations,
            owner: winner.owner
        });

        // Add to list for iteration
        nodePublicKeys[nodeCount] = winner.publicKey;
        nodeCount++;

        // Close opportunity
        opportunities[opportunityId].open = false;

        emit NodeApproved(opportunityId, candidate, winner.publicKey);
    }

    /**
     * @dev Remove a node (can only be called through governance).
     */
    function removeNode(string memory publicKey) public onlyGovernance {
        require(bytes(nodes[publicKey].publicKey).length > 0, "Node does not exist");

        // Find index
        uint256 indexToRemove = type(uint256).max;
        for (uint256 i = 0; i < nodeCount; i++) {
            if (keccak256(bytes(nodePublicKeys[i])) == keccak256(bytes(publicKey))) {
                indexToRemove = i;
                break;
            }
        }

        require(indexToRemove != type(uint256).max, "Node not found in list");

        // Replace with the last element
        if (indexToRemove != nodeCount - 1) {
            nodePublicKeys[indexToRemove] = nodePublicKeys[nodeCount - 1];
        }

        // Delete last element
        delete nodePublicKeys[nodeCount - 1];
        nodeCount--;

        // Delete node data and mark public key as available
        delete nodes[publicKey];
        delete usedPublicKeys[publicKey];

        emit NodeRemoved(publicKey);
    }

    /**
     * @dev Get all active nodes.
     */
    function getNodes() external view returns (Node[] memory) {
        Node[] memory activeNodes = new Node[](nodeCount);

        for (uint256 i = 0; i < nodeCount; i++) {
            string memory publicKey = nodePublicKeys[i];
            activeNodes[i] = nodes[publicKey];
        }

        return activeNodes;
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
