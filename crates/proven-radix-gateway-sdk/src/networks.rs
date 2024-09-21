use lazy_static::lazy_static;
use std::collections::HashMap;

// Using data from: https://github.com/radixdlt/babylon-gateway/blob/main/sdk/typescript/lib/helpers/networks.ts
// Removing duplicate network ids for rcnets

#[derive(Debug, Eq, Hash, PartialEq, Copy, Clone)]
pub enum Network {
    Mainnet = 0x01,
    Stokenet = 0x02,
    Alphanet = 0x0a,
    Betanet = 0x0b,
    Kisharnet = 0x0c,
    Zabanet = 0x0e,
    Gilganet = 0x20,
    Enkinet = 0x21,
    Hammunet = 0x22,
    Nergalnet = 0x23,
    Mardunet = 0x24,
    Dumunet = 0x25,
    LocalNet = 0xf0,
    InternalTestNet = 0xf1,
    Simulator = 0xf2,
}

impl Network {
    pub fn from_network_id(network_id: u8) -> Option<Self> {
        match network_id {
            0x01 => Some(Network::Mainnet),
            0x02 => Some(Network::Stokenet),
            0x0a => Some(Network::Alphanet),
            0x0b => Some(Network::Betanet),
            0x0c => Some(Network::Kisharnet),
            0x0e => Some(Network::Zabanet),
            0x20 => Some(Network::Gilganet),
            0x21 => Some(Network::Enkinet),
            0x22 => Some(Network::Hammunet),
            0x23 => Some(Network::Nergalnet),
            0x24 => Some(Network::Mardunet),
            0x25 => Some(Network::Dumunet),
            0xf0 => Some(Network::LocalNet),
            0xf1 => Some(Network::InternalTestNet),
            0xf2 => Some(Network::Simulator),
            _ => None,
        }
    }
}

pub struct NetworkConfig {
    pub network_name: &'static str,
    pub network_id: Network,
    pub gateway_url: &'static str,
    pub dashboard_url: &'static str,
}

lazy_static! {
    pub static ref RADIX_NETWORK_CONFIG: HashMap<Network, NetworkConfig> = {
        let mut m = HashMap::new();
        m.insert(
            Network::Mainnet,
            NetworkConfig {
                network_name: "Mainnet",
                network_id: Network::Mainnet,
                gateway_url: "https://mainnet.radixdlt.com",
                dashboard_url: "https://dashboard.radixdlt.com",
            },
        );
        m.insert(
            Network::Stokenet,
            NetworkConfig {
                network_name: "Stokenet",
                network_id: Network::Stokenet,
                gateway_url: "https://stokenet.radixdlt.com",
                dashboard_url: "https://dashboard.radixdlt.com",
            },
        );
        m.insert(
            Network::Kisharnet,
            NetworkConfig {
                network_name: "Kisharnet",
                network_id: Network::Kisharnet,
                gateway_url: "https://kisharnet-gateway.radixdlt.com",
                dashboard_url: "https://kisharnet-dashboard.radixdlt.com",
            },
        );
        m.insert(
            Network::Mardunet,
            NetworkConfig {
                network_name: "Mardunet",
                network_id: Network::Mardunet,
                gateway_url: "https://stokenet.radixdlt.com",
                dashboard_url: "https://dashboard.radixdlt.com",
            },
        );
        m.insert(
            Network::Zabanet,
            NetworkConfig {
                network_name: "Zabanet",
                network_id: Network::Zabanet,
                gateway_url: "https://zabanet-gateway.radixdlt.com",
                dashboard_url: "https://rcnet-v3-dashboard.radixdlt.com",
            },
        );
        m.insert(
            Network::Gilganet,
            NetworkConfig {
                network_name: "Gilganet",
                network_id: Network::Gilganet,
                gateway_url: "https://gilganet-gateway.radixdlt.com",
                dashboard_url: "https://gilganet-dashboard.rdx-works-main.extratools.works",
            },
        );
        m.insert(
            Network::Enkinet,
            NetworkConfig {
                network_name: "Enkinet",
                network_id: Network::Enkinet,
                gateway_url: "https://enkinet.radixdlt.com",
                dashboard_url: "https://gilganet-dashboard.rdx-works-main.extratools.work",
            },
        );
        m.insert(
            Network::Hammunet,
            NetworkConfig {
                network_name: "Hammunet",
                network_id: Network::Hammunet,
                gateway_url: "https://hammunet-gateway.radixdlt.com",
                dashboard_url: "https://hammunet-dashboard.rdx-works-main.extratools.works",
            },
        );
        m.insert(
            Network::Dumunet,
            NetworkConfig {
                network_name: "Dumunet",
                network_id: Network::Dumunet,
                gateway_url: "https://dumunet-gateway.radixdlt.com",
                dashboard_url: "https://dumunet-dashboard.rdx-works-main.extratools.works",
            },
        );

        m
    };
}
