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
        string fqdn;
        string publicKey;
        uint256 votes;
        string id;  // Unique identifier
    }

    struct Node {
        string id;
        string region;
        string availabilityZone;
        string fqdn;
        string publicKey;
        bytes32[] specializations;
        address owner;
    }

    // Storage
    mapping(bytes32 => NodeOpportunity) public opportunities;
    mapping(bytes32 => mapping(address => NodeCandidate)) public candidates;
    mapping(string => Node) public nodes;
    mapping(uint256 => string) public nodeIds; // Index to ID mapping for iteration
    uint256 public nodeCount = 0;

    // Events
    event OpportunityCreated(bytes32 id, string region, string availabilityZone);
    event CandidateRegistered(bytes32 opportunityId, address candidate, string fqdn);
    event NodeApproved(bytes32 opportunityId, address candidate, string nodeId);
    event NodeRemoved(string nodeId);

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
     * @dev Register as a candidate for a node opportunity.
     * This is allowed for anyone, not just governance, as individuals need to apply.
     */
    function registerAsCandidate(
        bytes32 opportunityId,
        string memory fqdn,
        string memory publicKey
    ) external {
        require(opportunities[opportunityId].open, "Opportunity is closed");

        candidates[opportunityId][msg.sender] = NodeCandidate({
            owner: msg.sender,
            fqdn: fqdn,
            publicKey: publicKey,
            votes: 0,
            id: string(abi.encodePacked(opportunityId, "-", msg.sender))
        });

        emit CandidateRegistered(opportunityId, msg.sender, fqdn);
    }

    /**
     * @dev Approve a node candidate (can only be called through governance).
     */
    function approveCandidate(bytes32 opportunityId, address candidate) public onlyGovernance {
        require(candidates[opportunityId][candidate].owner != address(0), "Candidate does not exist");
        require(opportunities[opportunityId].open, "Opportunity is closed");

        NodeCandidate memory winner = candidates[opportunityId][candidate];
        NodeOpportunity memory opportunity = opportunities[opportunityId];

        string memory nodeId = winner.id;

        nodes[nodeId] = Node({
            id: nodeId,
            region: opportunity.region,
            availabilityZone: opportunity.availabilityZone,
            fqdn: winner.fqdn,
            publicKey: winner.publicKey,
            specializations: opportunity.specializations,
            owner: winner.owner
        });

        // Add to list for iteration
        nodeIds[nodeCount] = nodeId;
        nodeCount++;

        // Close opportunity
        opportunities[opportunityId].open = false;

        emit NodeApproved(opportunityId, candidate, nodeId);
    }

    /**
     * @dev Remove a node (can only be called through governance).
     */
    function removeNode(string memory nodeId) public onlyGovernance {
        require(bytes(nodes[nodeId].id).length > 0, "Node does not exist");

        // Find index
        uint256 indexToRemove = type(uint256).max;
        for (uint256 i = 0; i < nodeCount; i++) {
            if (keccak256(bytes(nodeIds[i])) == keccak256(bytes(nodeId))) {
                indexToRemove = i;
                break;
            }
        }

        require(indexToRemove != type(uint256).max, "Node not found in list");

        // Replace with the last element
        if (indexToRemove != nodeCount - 1) {
            nodeIds[indexToRemove] = nodeIds[nodeCount - 1];
        }

        // Delete last element
        delete nodeIds[nodeCount - 1];
        nodeCount--;

        // Delete node data
        delete nodes[nodeId];

        emit NodeRemoved(nodeId);
    }

    /**
     * @dev Get all active nodes.
     */
    function getNodes() external view returns (Node[] memory) {
        Node[] memory activeNodes = new Node[](nodeCount);

        for (uint256 i = 0; i < nodeCount; i++) {
            string memory nodeId = nodeIds[i];
            activeNodes[i] = nodes[nodeId];
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
