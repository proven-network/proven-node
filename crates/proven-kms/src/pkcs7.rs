#![allow(dead_code)]
use anyhow::{anyhow, Result};
use asn1_rs::{oid, BerSequence};
use asn1_rs::{
    Any, Class, FromBer, Integer, OctetString, Oid, OptTaggedParser, SetOf, Tag, Tagged,
};
use cbc::cipher::crypto_common::KeyIvInit;
use cbc::cipher::{block_padding, BlockDecryptMut};
use rsa::oaep::Oaep;
use rsa::RsaPrivateKey;
use sha2::Sha256;

type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;

const OID_NIST_SHA_256: Oid<'static> = oid!(2.16.840 .1 .101 .3 .4 .2 .1);
const OID_NIST_AES256_CBC: Oid<'static> = oid!(2.16.840 .1 .101 .3 .4 .1 .42);
const OID_PKCS1_RSA_OAEP: Oid<'static> = oid!(1.2.840 .113549 .1 .1 .7);
const OID_PKCS1_MGF: Oid<'static> = oid!(1.2.840 .113549 .1 .1 .8);
const OID_PKCS7_ENVELOPED_DATA: Oid<'static> = oid!(1.2.840 .113549 .1 .7 .3);
const OID_PKCS7_DATA: Oid<'static> = oid!(1.2.840 .113549 .1 .7 .1);

/*
ContentInfo ::= SEQUENCE {
  contentType ContentType,
  content [0] EXPLICIT ANY DEFINED BY contentType }
*/

#[derive(BerSequence, Debug)]
pub struct ContentInfo<'a> {
    pub content_type: Oid<'a>,

    #[tag_explicit(0)]
    pub content: EnvelopedData<'a>,
}

impl<'a> ContentInfo<'a> {
    pub fn parse_ber(ber: &'a [u8]) -> Result<Self> {
        let (rem, ci) = Self::from_ber(ber)?;

        if !rem.is_empty() {
            return Err(anyhow!(
                "trailing {} bytes after parsing ContentInfo",
                rem.len()
            ));
        }

        ci.validate()?;

        Ok(ci)
    }

    fn validate(&self) -> Result<()> {
        if self.content_type != OID_PKCS7_ENVELOPED_DATA {
            return Err(anyhow!(
                "unexpected content type: {}, expected {}",
                self.content_type,
                OID_PKCS7_ENVELOPED_DATA
            ));
        }

        self.content.validate()
    }

    pub fn decrypt_content(&self, priv_key: &RsaPrivateKey) -> Result<Vec<u8>> {
        let datakey = self.decrypt_key(priv_key)?;
        self.content
            .encrypted_content_info
            .decrypt_content(&datakey)
    }

    fn decrypt_key(&self, priv_key: &RsaPrivateKey) -> Result<Vec<u8>> {
        let ciphertext = self
            .content
            .recipient_infos
            .iter()
            .next()
            .unwrap()
            .encrypted_key
            .as_ref();

        let padding = Oaep::new_with_mgf_hash::<Sha256, Sha256>();

        Ok(priv_key.decrypt(padding, ciphertext)?)
    }
}

/*
EnvelopedData ::= SEQUENCE {
  version CMSVersion,
  originatorInfo [0] IMPLICIT OriginatorInfo OPTIONAL,
  recipientInfos RecipientInfos,
  encryptedContentInfo EncryptedContentInfo,
  unprotectedAttrs [1] IMPLICIT UnprotectedAttributes OPTIONAL }

RecipientInfos ::= SET SIZE (1..MAX) OF RecipientInfo
*/

#[derive(BerSequence, Debug)]
pub struct EnvelopedData<'a> {
    pub version: Integer<'a>,

    #[optional]
    #[tag_implicit(0)]
    pub originator_info: Option<OriginatorInfo<'a>>,

    pub recipient_infos: SetOf<KeyTransRecipientInfo<'a>>,

    pub encrypted_content_info: EncryptedContentInfo<'a>,

    #[optional]
    #[tag_implicit(1)]
    pub unprotected_attrs: Option<SetOf<Attribute<'a>>>,
}

impl<'a> EnvelopedData<'a> {
    fn validate(&self) -> Result<()> {
        let ver = self.version.as_i32()?;
        if ver != 2 {
            return Err(anyhow!(
                "unexpected EnvelopedData.version: {ver}, expected 2"
            ));
        }

        if self.recipient_infos.len() != 1 {
            return Err(anyhow!(
                "unexpected EnvelopedData.recipient_infos length: {}, expected 1",
                self.recipient_infos.len()
            ));
        }

        self.recipient_infos.iter().next().unwrap().validate()?;

        self.encrypted_content_info.validate()
    }
}

/*
OriginatorInfo ::= SEQUENCE {
  certs [0] IMPLICIT CertificateSet OPTIONAL,
  crls [1] IMPLICIT RevocationInfoChoices OPTIONAL }
*/

#[derive(BerSequence, Debug)]
pub struct OriginatorInfo<'a> {
    #[optional]
    #[tag_implicit(0)]
    pub certs: Option<SetOf<Any<'a>>>,

    #[optional]
    #[tag_implicit(1)]
    pub crls: Option<SetOf<Any<'a>>>,
}

/*
RecipientInfos ::= SET SIZE (1..MAX) OF RecipientInfo

RecipientInfo ::= CHOICE {
  ktri KeyTransRecipientInfo,
  kari [1] KeyAgreeRecipientInfo,
  kekri [2] KEKRecipientInfo,
  pwri [3] PasswordRecipientinfo,
  ori [4] OtherRecipientInfo }

KeyTransRecipientInfo ::= SEQUENCE {
  version CMSVersion,  -- always set to 0 or 2
  rid RecipientIdentifier,
  keyEncryptionAlgorithm KeyEncryptionAlgorithmIdentifier,
  encryptedKey EncryptedKey }
*/

#[derive(BerSequence, Debug)]
pub struct KeyTransRecipientInfo<'a> {
    pub version: Integer<'a>,
    pub rid: Any<'a>,
    pub key_encryption_algorithm: AlgorithmIdentifier<'a>,
    pub encrypted_key: OctetString<'a>,
}

impl<'a> KeyTransRecipientInfo<'a> {
    fn validate(&self) -> Result<()> {
        let ver = self.version.as_i32()?;
        if ver != 2 {
            return Err(anyhow!(
                "unexpected KeyTransRecipientInfo.version: {ver}, expected 2"
            ));
        }

        let key_algo = &self.key_encryption_algorithm;

        if key_algo.algorithm != OID_PKCS1_RSA_OAEP {
            return Err(anyhow!("unexpected KeyTransRecipientInfo.key_encryption_algorithm.algorithm: {}, expected {OID_PKCS1_RSA_OAEP}",
                key_algo.algorithm));
        }

        if let Some(ref params) = key_algo.parameters {
            let rsa_oaep_params: RsaesOaepParameters<'a> = params.clone().try_into()?;
            rsa_oaep_params.validate()?;
        } else {
            return Err(anyhow!(
                "Missing KeyTransRecipientInfo.key_encryption_algorithm.parameters"
            ));
        }

        Ok(())
    }
}

/*
RecipientIdentifier ::= CHOICE {
  issuerAndSerialNumber IssuerAndSerialNumber,
  subjectKeyIdentifier [0] SubjectKeyIdentifier }

AlgorithmIdentifier{ALGORITHM-TYPE, ALGORITHM-TYPE:AlgorithmSet} ::=
  SEQUENCE {
    algorithm   ALGORITHM-TYPE.&id({AlgorithmSet}),
      parameters  ALGORITHM-TYPE.
             &Params({AlgorithmSet}{@algorithm}) OPTIONAL
  }

EncryptedKey ::= OCTET STRING
*/

#[derive(BerSequence, Debug)]
pub struct AlgorithmIdentifier<'a> {
    pub algorithm: Oid<'a>,

    #[optional]
    pub parameters: Option<Any<'a>>,
}

/*
RSAES-OAEP-params  ::=  SEQUENCE  {
  hashFunc    [0] AlgorithmIdentifier DEFAULT sha1Identifier,
  maskGenFunc [1] AlgorithmIdentifier DEFAULT mgf1SHA1Identifier,
  pSourceFunc [2] AlgorithmIdentifier DEFAULT
                    pSpecifiedEmptyIdentifier  }
*/

#[derive(Debug)]
pub struct RsaesOaepParameters<'a> {
    hash_alg: Option<AlgorithmIdentifier<'a>>,
    mask_gen_alg: Option<AlgorithmIdentifier<'a>>,
    _p_source_alg: Option<AlgorithmIdentifier<'a>>,
}

impl<'a> RsaesOaepParameters<'a> {
    fn validate(&self) -> Result<()> {
        if let Some(ref alg) = self.hash_alg {
            if alg.algorithm != OID_NIST_SHA_256 {
                return Err(anyhow!("unexpected KeyTransRecipientInfo.key_encryption_algorithm.hash_func: {}, expected {OID_NIST_SHA_256}",
                    alg.algorithm));
            }
        } else {
            return Err(anyhow!("missing KeyTransRecipientInfo.key_encryption_algorithm.hash_func, expected {OID_NIST_SHA_256}"));
        }

        if let Some(ref alg) = self.mask_gen_alg {
            if alg.algorithm != OID_PKCS1_MGF {
                return Err(anyhow!("unexpected KeyTransRecipientInfo.key_encryption_algorithm.mask_gen_func: {}, expected {OID_PKCS1_MGF}",
                    alg.algorithm));
            }

            if let Some(ref params) = alg.parameters {
                let (_, mgf_hash) = Oid::from_ber(params.as_bytes())?;
                if mgf_hash != OID_NIST_SHA_256 {
                    return Err(anyhow!("unexpected KeyTransRecipientInfo.key_encryption_algorithm.mask_gen_func.hash: {}, expected {OID_NIST_SHA_256}",
                        mgf_hash));
                }
            } else {
                return Err(anyhow!("missing KeyTransRecipientInfo.key_encryption_algorithm.mask_gen_func.parameters"));
            }
        } else {
            return Err(anyhow!("missing KeyTransRecipientInfo.key_encryption_algorithm.parameters.mask_gen_func, expected {OID_PKCS1_MGF}"));
        }

        Ok(())
    }
}

impl<'a> TryFrom<Any<'a>> for RsaesOaepParameters<'a> {
    type Error = asn1_rs::Error;

    fn try_from(value: Any<'a>) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

//     hashFunc          [0] AlgorithmIdentifier DEFAULT
//                              sha1Identifier,
//     maskGenFunc       [1] AlgorithmIdentifier DEFAULT
//                              mgf1SHA1Identifier,
//     pSourceFunc       [2] AlgorithmIdentifier DEFAULT
//                              pSpecifiedEmptyIdentifier  }
impl<'a, 'b> TryFrom<&'b Any<'a>> for RsaesOaepParameters<'a> {
    type Error = asn1_rs::Error;

    fn try_from(value: &'b Any<'a>) -> Result<Self, Self::Error> {
        value.tag().assert_eq(Tag::Sequence)?;
        let i = &value.data;

        let (i, hash_alg) = OptTaggedParser::new(Class::ContextSpecific, Tag(0))
            .parse_ber(i, |_, inner| AlgorithmIdentifier::from_ber(inner))?;

        let (i, mask_gen_alg) = OptTaggedParser::new(Class::ContextSpecific, Tag(1))
            .parse_ber(i, |_, inner| AlgorithmIdentifier::from_ber(inner))?;

        let (_, p_source_alg) = OptTaggedParser::new(Class::ContextSpecific, Tag(2))
            .parse_ber(i, |_, inner| AlgorithmIdentifier::from_ber(inner))?;

        Ok(Self {
            hash_alg,
            mask_gen_alg,
            _p_source_alg: p_source_alg,
        })
    }
}

pub type Aes256CBCParameter<'a> = OctetString<'a>;

/*
EncryptedContentInfo ::= SEQUENCE {
  contentType ContentType,
  contentEncryptionAlgorithm ContentEncryptionAlgorithmIdentifier,
  encryptedContent [0] IMPLICIT EncryptedContent OPTIONAL }
*/

#[derive(BerSequence, Debug)]
pub struct EncryptedContentInfo<'a> {
    pub content_type: Oid<'a>,
    pub content_encryption_algorithm: AlgorithmIdentifier<'a>,
    pub encrypted_content: Any<'a>,
}

impl<'a> EncryptedContentInfo<'a> {
    fn validate(&self) -> Result<()> {
        if self.content_type != OID_PKCS7_DATA {
            return Err(anyhow!(
                "unexpected EncryptedContentInfo.content_type: {}, expected {OID_PKCS7_DATA}",
                self.content_type
            ));
        }

        if self.content_encryption_algorithm.algorithm != OID_NIST_AES256_CBC {
            return Err(anyhow!("unexpected EncryptedContentInfo.content_encryption_algorithm: {}, expected {OID_NIST_AES256_CBC}",
                    self.content_encryption_algorithm.algorithm));
        }

        // Ignoring the OPTIONAL directive, it should always be there in our use case
        let any = &self.encrypted_content;

        if any.header.class() != Class::ContextSpecific {
            return Err(anyhow!(
                "unexpected EncryptedContentInfo.encrypted_content.class: {}, expected {}",
                any.header.class(),
                Class::ContextSpecific
            ));
        }

        if any.header.tag().0 != 0 {
            return Err(anyhow!(
                "unexpected EncryptedContentInfo.encrypted_content.tag: {}, expected 0",
                any.header.tag().0
            ));
        }

        Ok(())
    }

    fn decrypt_content(&self, datakey: &[u8]) -> Result<Vec<u8>> {
        let iv: Aes256CBCParameter = self
            .content_encryption_algorithm
            .parameters
            .as_ref()
            .unwrap()
            .try_into()
            .unwrap();

        let ciphertext = self.combined_content()?;
        let dec = Aes256CbcDec::new(datakey.into(), iv.as_ref().into());
        Ok(dec
            .decrypt_padded_vec_mut::<block_padding::Pkcs7>(&ciphertext)
            .unwrap())
    }

    fn combined_content(&self) -> Result<Vec<u8>> {
        // Ignoring the OPTIONAL directive, it should always be there in our use case
        let any = &self.encrypted_content;

        if any.header.is_constructed() {
            let mut data = any.data;
            let mut combined = Vec::new();

            while !data.is_empty() {
                // concatentate the inner parts
                let (rem, part) = OctetString::from_ber(data)?;
                combined.extend_from_slice(part.as_ref());
                data = rem;
            }

            Ok(combined)
        } else {
            let octets: OctetString = any.try_into()?;
            Ok(octets.as_ref().to_vec())
        }
    }
}
/*
Attribute ::= SEQUENCE {
  attrType OBJECT IDENTIFIER,
  attrValues SET OF AttributeValue }
*/

#[derive(BerSequence, Debug)]
pub struct Attribute<'a> {
    pub attr_type: Oid<'a>,
    pub attr_values: SetOf<Any<'a>>,
}
