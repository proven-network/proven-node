mod error;

use std::net::Ipv4Addr;

pub use error::{Error, Result};

use aws_config::Region;

#[derive(Clone, Debug)]
pub struct Instance {
    pub instance_id: String,
    pub availability_zone: String,
    pub private_ip: Ipv4Addr,
    pub public_ip: Option<Ipv4Addr>,
    pub vpc_id: String,
    pub subnet_id: String,
    pub security_group_ids: Vec<String>,
    pub profile_arn: String,
}

#[derive(Clone, Debug)]
pub struct InstanceDetailsFetcher {
    client: aws_sdk_ec2::Client,
}

impl InstanceDetailsFetcher {
    pub async fn new(region: String) -> Self {
        let config = aws_config::from_env()
            .region(Region::new(region))
            .load()
            .await;

        Self {
            client: aws_sdk_ec2::Client::new(&config),
        }
    }

    pub async fn get_instance_details(&self, instance_id: String) -> Result<Instance> {
        let resp = self
            .client
            .describe_instances()
            .instance_ids(instance_id)
            .send()
            .await;

        match resp {
            Ok(resp) => {
                let reservations = resp.reservations.unwrap_or_default();
                let instances = reservations
                    .into_iter()
                    .flat_map(|r| r.instances.unwrap_or_default());
                let instance = instances.into_iter().next();
                match instance {
                    Some(i) => Ok(Instance {
                        instance_id: i.instance_id.ok_or(Error::MissingDetails)?,
                        availability_zone: i
                            .placement
                            .ok_or(Error::MissingDetails)?
                            .availability_zone
                            .ok_or(Error::MissingDetails)?,
                        private_ip: i.private_ip_address.ok_or(Error::MissingDetails)?.parse()?,
                        public_ip: i.public_ip_address.map(|ip| ip.parse()).transpose()?,
                        vpc_id: i.vpc_id.ok_or(Error::MissingDetails)?,
                        subnet_id: i.subnet_id.ok_or(Error::MissingDetails)?,
                        security_group_ids: i
                            .security_groups
                            .unwrap_or_default()
                            .into_iter()
                            .map(|sg| sg.group_id.ok_or(Error::MissingDetails))
                            .collect::<Result<Vec<String>>>()?,
                        profile_arn: i
                            .iam_instance_profile
                            .ok_or(Error::MissingDetails)?
                            .arn
                            .ok_or(Error::MissingDetails)?,
                    }),
                    None => Err(Error::InstanceNotFound),
                }
            }
            Err(e) => Err(Error::EC2(e.into())),
        }
    }
}
