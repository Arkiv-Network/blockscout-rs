use std::fs;

pub const FIRST_PAGE_RESPONSE: &str = r#"
{
    "CrossChainTx": [
        {
            "creator": "",
            "index": "0x36b9bb2f1d745b41d8e64eb203752c02abe6f50c5e932563799d5cbf160f5117",
            "zeta_fees": "10959181044563",
            "relayed_message": "",
            "cctx_status": {
                "status": "OutboundMined",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750344444",
                "isAbortRefunded": false,
                "created_timestamp": "1750344018",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "0x73B37B8BAbAC0e846bB2C4c581e60bFF2BFBE76e",
                "sender_chain_id": "7001",
                "tx_origin": "0x07Db16a4c71DD0467994e7f0dC121E7D96d36c8A",
                "coin_type": "Zeta",
                "asset": "",
                "amount": "100000000000000000",
                "observed_hash": "0xcbe2d7febc3dcebae09af25a84d138f095d4a6f8c949cd80276c663c1b62a6e6",
                "observed_external_height": "10966606",
                "ballot_index": "0x36b9bb2f1d745b41d8e64eb203752c02abe6f50c5e932563799d5cbf160f5117",
                "finalized_zeta_height": "0",
                "tx_finalization_status": "NotFinalized",
                "is_cross_chain_call": false,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "0x07db16a4c71dd0467994e7f0dc121e7d96d36c8a",
                    "receiver_chainId": "11155111",
                    "coin_type": "Zeta",
                    "amount": "99989040818955437",
                    "tss_nonce": "2881",
                    "gas_limit": "0",
                    "gas_price": "2400616",
                    "gas_priority_fee": "0",
                    "hash": "0xb80b0b22b5392d8d5ef04273e73fefb7f94b25a99e66dee3317b6071e49acf5a",
                    "ballot_index": "0x27245f0d719c8bfcb9192dad814b8550a0d0191426935b1d93a55fdd6b081801",
                    "observed_external_height": "8583496",
                    "gas_used": "48129",
                    "effective_gas_price": "2400616",
                    "effective_gas_limit": "100000",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "Executed",
                    "call_options": {
                        "gas_limit": "90000",
                        "is_arbitrary_call": false
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V1",
            "revert_options": {
                "revert_address": "",
                "call_on_revert": false,
                "abort_address": "",
                "revert_message": null,
                "revert_gas_limit": "0"
            }
        },
        {
            "creator": "",
            "index": "0x3c61b9bb605136895bec5c6adacaabb85f08a3692d486d7d1849f5e8010eb74b",
            "zeta_fees": "0",
            "relayed_message": "33633663646232653832383065613663326337623539386662323037623536653464363830653963343635326435323163383538343465613365306535373465653330656635336131633232623363306362353937306134653133336133383132323733",
            "cctx_status": {
                "status": "OutboundMined",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750344144",
                "isAbortRefunded": false,
                "created_timestamp": "1750343826",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "0x5e55d01aC8677f70ddd437d647b3548456871123",
                "sender_chain_id": "7001",
                "tx_origin": "0x5e55d01aC8677f70ddd437d647b3548456871123",
                "coin_type": "ERC20",
                "asset": "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
                "amount": "1000",
                "observed_hash": "0xa6ccbbc42159f92949cef8304a0ef6f39d11ba127038a29cd639fb8ef68716e8",
                "observed_external_height": "10966560",
                "ballot_index": "0x3c61b9bb605136895bec5c6adacaabb85f08a3692d486d7d1849f5e8010eb74b",
                "finalized_zeta_height": "0",
                "tx_finalization_status": "NotFinalized",
                "is_cross_chain_call": true,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "0x65e9Bd75449DFE94fF7a627e81084E37f3E5C422",
                    "receiver_chainId": "11155111",
                    "coin_type": "ERC20",
                    "amount": "1000",
                    "tss_nonce": "2880",
                    "gas_limit": "0",
                    "gas_price": "1200183",
                    "gas_priority_fee": "0",
                    "hash": "0x18ea3bafce2f25fbcbb337c749993b4014328cc809b64a8f5dfb1f80c1f8b692",
                    "ballot_index": "0x8a471ed21254eccb72ad933cc609bb100339e6902bfa9fc1140c27955e1fee20",
                    "observed_external_height": "8583473",
                    "gas_used": "172123",
                    "effective_gas_price": "1200183",
                    "effective_gas_limit": "300000",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "Executed",
                    "call_options": {
                        "gas_limit": "300000",
                        "is_arbitrary_call": false
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V2",
            "revert_options": {
                "revert_address": "0x0000000000000000000000000000000000000000",
                "call_on_revert": false,
                "abort_address": "0x0000000000000000000000000000000000000000",
                "revert_message": null,
                "revert_gas_limit": "0"
            }
        },
        {
            "creator": "zeta1dxyzsket66vt886ap0gnzlnu5pv0y99v086wnz",
            "index": "0xa43b4573e40da5c36e06ab01b24a3b3e2c5cf223760471d76177b681cf625638",
            "zeta_fees": "0",
            "relayed_message": "61343332613062373065656332366466333730393764353964316135646439333765646366393333323965333365623665636436383035623831363831633261636663396438656563636635343262633639326139313837613666346639396331653463",
            "cctx_status": {
                "status": "OutboundMined",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750343808",
                "isAbortRefunded": false,
                "created_timestamp": "1750343808",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "0x5e55d01aC8677f70ddd437d647b3548456871123",
                "sender_chain_id": "11155111",
                "tx_origin": "0x5e55d01aC8677f70ddd437d647b3548456871123",
                "coin_type": "ERC20",
                "asset": "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238",
                "amount": "1000",
                "observed_hash": "0x698d62baa3ae209207f6a74a3d4e51c3c73e7207f41f3a356f8459110004aafc",
                "observed_external_height": "8583445",
                "ballot_index": "0xa43b4573e40da5c36e06ab01b24a3b3e2c5cf223760471d76177b681cf625638",
                "finalized_zeta_height": "10966556",
                "tx_finalization_status": "Executed",
                "is_cross_chain_call": true,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "0x9EfeB21fdFC99B640187C45155DC84ebEF47104f",
                    "receiver_chainId": "7001",
                    "coin_type": "ERC20",
                    "amount": "0",
                    "tss_nonce": "0",
                    "gas_limit": "0",
                    "gas_price": "",
                    "gas_priority_fee": "",
                    "hash": "0xf43eb574b701f0b6eb50f0e8b7638867fe5ef81608d20b84ea17548a8024a11b",
                    "ballot_index": "",
                    "observed_external_height": "10966556",
                    "gas_used": "0",
                    "effective_gas_price": "0",
                    "effective_gas_limit": "0",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "Executed",
                    "call_options": {
                        "gas_limit": "1500000",
                        "is_arbitrary_call": false
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V2",
            "revert_options": {
                "revert_address": "0x0000000000000000000000000000000000000000",
                "call_on_revert": false,
                "abort_address": "0x0000000000000000000000000000000000000000",
                "revert_message": null,
                "revert_gas_limit": "0"
            }
        }
    ],
    "pagination": {
        "next_key": "SECOND_PAGE",
        "total": "0"
    }
}
"#;

pub const SECOND_PAGE_RESPONSE: &str = r#"
{
    "CrossChainTx": [
        {
            "creator": "zeta1j8g8ch4uqgl3gtet3nntvczaeppmlxajqwh5u6",
            "index": "0xff18366b92db12f7204eca924bc8ffbf93f655c7a35b68ec38b38d61d49fbaeb",
            "zeta_fees": "0",
            "relayed_message": "000000000000000000000000f255835ca48cd5b2ca37771a158922e2575ebc490000000000000000000000007cb5e8e39cbf751cbcfbf8a4a80fc8ad04fdf01698dcea32f8e9b565d70d98dcca59b26a68d78cac896314ea993730640fe8e864",
            "cctx_status": {
                "status": "OutboundMined",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750350182",
                "isAbortRefunded": false,
                "created_timestamp": "1750350182",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "0x071D7eF713b3c3b4bc22b279E7cE3770F9B4f4B9",
                "sender_chain_id": "11155111",
                "tx_origin": "0x071D7eF713b3c3b4bc22b279E7cE3770F9B4f4B9",
                "coin_type": "NoAssetCall",
                "asset": "",
                "amount": "0",
                "observed_hash": "0xe8a1469f07e853a5e995668194ba456c1e52e827aefa8a92d3eead9c8c19b870",
                "observed_external_height": "8583972",
                "ballot_index": "0xff18366b92db12f7204eca924bc8ffbf93f655c7a35b68ec38b38d61d49fbaeb",
                "finalized_zeta_height": "10968070",
                "tx_finalization_status": "Executed",
                "is_cross_chain_call": false,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "0xD8d6f3ac69c0903A3fe8d2c0c1A2dE4DC989374b",
                    "receiver_chainId": "7001",
                    "coin_type": "NoAssetCall",
                    "amount": "0",
                    "tss_nonce": "0",
                    "gas_limit": "0",
                    "gas_price": "",
                    "gas_priority_fee": "",
                    "hash": "0x523f02f42ae65ed8ef56844bd56bb9ec06213679604a33eedaaa1465f6179a9c",
                    "ballot_index": "",
                    "observed_external_height": "10968070",
                    "gas_used": "0",
                    "effective_gas_price": "0",
                    "effective_gas_limit": "0",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "Executed",
                    "call_options": {
                        "gas_limit": "1500000",
                        "is_arbitrary_call": false
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V2",
            "revert_options": {
                "revert_address": "0x071D7eF713b3c3b4bc22b279E7cE3770F9B4f4B9",
                "call_on_revert": false,
                "abort_address": "0x0000000000000000000000000000000000000000",
                "revert_message": "AAAAAAAAAAAAAAAA8lWDXKSM1bLKN3caFYki4ldevEkAAAAAAAAAAAAAAAB8tejjnL91HLz7+KSoD8itBP3wFpjc6jL46bVl1w2Y3MpZsmpo14ysiWMU6pk3MGQP6Ohk",
                "revert_gas_limit": "0"
            }
        },
        {
            "creator": "zeta1j8g8ch4uqgl3gtet3nntvczaeppmlxajqwh5u6",
            "index": "0x60d7e3b6d06a7deb38217c317f707bfbbb4a87b03fa6813eaea27448f71e438d",
            "zeta_fees": "0",
            "relayed_message": "000000000000000000000000f33e7b4c86661214346f974b4f7a9bd086bc72d3000000000000000000000000b0a1c893e6f6dd165d8f77f157aac9caf489f74c98dcea32f8e9b565d70d98dcca59b26a68d78cac896314ea993730640fe8e864",
            "cctx_status": {
                "status": "OutboundMined",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750349701",
                "isAbortRefunded": false,
                "created_timestamp": "1750349701",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "0x2914c4b9079e4892DF38D2CD8f964f3E64422dC3",
                "sender_chain_id": "11155111",
                "tx_origin": "0x2914c4b9079e4892DF38D2CD8f964f3E64422dC3",
                "coin_type": "NoAssetCall",
                "asset": "",
                "amount": "0",
                "observed_hash": "0x4aadbde3292132cd26a9259860e90acd1063b4537918a58ae307909bf2a0b7f0",
                "observed_external_height": "8583933",
                "ballot_index": "0x60d7e3b6d06a7deb38217c317f707bfbbb4a87b03fa6813eaea27448f71e438d",
                "finalized_zeta_height": "10967956",
                "tx_finalization_status": "Executed",
                "is_cross_chain_call": false,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "0xD8d6f3ac69c0903A3fe8d2c0c1A2dE4DC989374b",
                    "receiver_chainId": "7001",
                    "coin_type": "NoAssetCall",
                    "amount": "0",
                    "tss_nonce": "0",
                    "gas_limit": "0",
                    "gas_price": "",
                    "gas_priority_fee": "",
                    "hash": "0x6d9882defc163d9710ef677989051fa7e18ae45bd544f3eda1816b82e0af1458",
                    "ballot_index": "",
                    "observed_external_height": "10967956",
                    "gas_used": "0",
                    "effective_gas_price": "0",
                    "effective_gas_limit": "0",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "Executed",
                    "call_options": {
                        "gas_limit": "1500000",
                        "is_arbitrary_call": false
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V2",
            "revert_options": {
                "revert_address": "0x2914c4b9079e4892DF38D2CD8f964f3E64422dC3",
                "call_on_revert": false,
                "abort_address": "0x0000000000000000000000000000000000000000",
                "revert_message": "AAAAAAAAAAAAAAAA8z57TIZmEhQ0b5dLT3qb0Ia8ctMAAAAAAAAAAAAAAACwociT5vbdFl2Pd/FXqsnK9In3TJjc6jL46bVl1w2Y3MpZsmpo14ysiWMU6pk3MGQP6Ohk",
                "revert_gas_limit": "0"
            }
        },
        {
            "creator": "zeta1mte0r3jzkf2rkd7ex4p3xsd3fxqg7q29q0wxl5",
            "index": "0xf79685f9581b10914080dba78514c1fea3003fc6d68a5b92cb359d18d9b67407",
            "zeta_fees": "0",
            "relayed_message": "",
            "cctx_status": {
                "status": "OutboundMined",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750344953",
                "isAbortRefunded": false,
                "created_timestamp": "1750344953",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "0x71973ec13be525f912b1bcda5d631f10388cb13cd2202b7b8650d4d6fe4b2339",
                "sender_chain_id": "103",
                "tx_origin": "0x71973ec13be525f912b1bcda5d631f10388cb13cd2202b7b8650d4d6fe4b2339",
                "coin_type": "Gas",
                "asset": "",
                "amount": "1000000",
                "observed_hash": "9H1aPvzrUTTaXHGKVXesCecnWMRZaBGKjtgWeTk6hfXs",
                "observed_external_height": "209702944",
                "ballot_index": "0xf79685f9581b10914080dba78514c1fea3003fc6d68a5b92cb359d18d9b67407",
                "finalized_zeta_height": "10966828",
                "tx_finalization_status": "Executed",
                "is_cross_chain_call": false,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "0x4955a3F38ff86ae92A914445099caa8eA2B9bA32",
                    "receiver_chainId": "7001",
                    "coin_type": "Gas",
                    "amount": "0",
                    "tss_nonce": "0",
                    "gas_limit": "0",
                    "gas_price": "",
                    "gas_priority_fee": "",
                    "hash": "0x4a3e77ab9536c74b8321afd1190492dfddc6eeff8f2fb108b7dcf7a0c37471b4",
                    "ballot_index": "",
                    "observed_external_height": "10966828",
                    "gas_used": "0",
                    "effective_gas_price": "0",
                    "effective_gas_limit": "0",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "Executed",
                    "call_options": {
                        "gas_limit": "1500000",
                        "is_arbitrary_call": false
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V2",
            "revert_options": {
                "revert_address": "",
                "call_on_revert": false,
                "abort_address": "",
                "revert_message": null,
                "revert_gas_limit": "0"
            }
        }
    ],
    "pagination": {
        "next_key": "THIRD_PAGE",
        "total": "0"
    }
}
"#;

pub const THIRD_PAGE_RESPONSE: &str = r#"
{
    "CrossChainTx": [
        {
            "creator": "zeta18pksjzclks34qkqyaahf2rakss80mnusju77cm",
            "index": "0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31",
            "zeta_fees": "0",
            "relayed_message": "",
            "cctx_status": {
                "status": "OutboundMined",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750344684",
                "isAbortRefunded": false,
                "created_timestamp": "1750344684",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "tb1q99jmq3q5s9hzm65vg0lyk3eqht63ssw8uyzy67",
                "sender_chain_id": "18333",
                "tx_origin": "tb1q99jmq3q5s9hzm65vg0lyk3eqht63ssw8uyzy67",
                "coin_type": "Gas",
                "asset": "",
                "amount": "8504",
                "observed_hash": "ed64294c274f4c8204f9c8e8495495b839c59d3944bfcf51ec8ad6d3d5721e38",
                "observed_external_height": "257082",
                "ballot_index": "0x7f70bf83ed66c8029d8b2fce9ca95a81d053243537d0ea694de5a9c8e7d42f31",
                "finalized_zeta_height": "10966764",
                "tx_finalization_status": "Executed",
                "is_cross_chain_call": false,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "0x33c2f2B93798629f1311cA9ade3D4BF732011718",
                    "receiver_chainId": "7001",
                    "coin_type": "Gas",
                    "amount": "0",
                    "tss_nonce": "0",
                    "gas_limit": "0",
                    "gas_price": "",
                    "gas_priority_fee": "",
                    "hash": "0xcd6c1391ca9950bb527b36b21ac75bccd29e7a2adf105662cb5eadd4bda5b4d5",
                    "ballot_index": "",
                    "observed_external_height": "10966764",
                    "gas_used": "0",
                    "effective_gas_price": "0",
                    "effective_gas_limit": "0",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "Executed",
                    "call_options": {
                        "gas_limit": "0",
                        "is_arbitrary_call": false
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V2",
            "revert_options": {
                "revert_address": "",
                "call_on_revert": false,
                "abort_address": "",
                "revert_message": null,
                "revert_gas_limit": "0"
            }
        },
        {
            "creator": "",
            "index": "0xf7b98c51a222d1499001eaa98004d7ee6ebebbdc46597d2d03197e8b0e7e16b2",
            "zeta_fees": "0",
            "relayed_message": "",
            "cctx_status": {
                "status": "OutboundMined",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750344706",
                "isAbortRefunded": false,
                "created_timestamp": "1750344287",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "0x58A8Ba18c585C411B95Ba1e78962a2A3E1c6f52a",
                "sender_chain_id": "7001",
                "tx_origin": "0x58A8Ba18c585C411B95Ba1e78962a2A3E1c6f52a",
                "coin_type": "Gas",
                "asset": "",
                "amount": "900000",
                "observed_hash": "0xa6e706fb088fb81598697da4eb0efccb8a6e4714afca6e8237bacee543b2bf4a",
                "observed_external_height": "10966670",
                "ballot_index": "0xf7b98c51a222d1499001eaa98004d7ee6ebebbdc46597d2d03197e8b0e7e16b2",
                "finalized_zeta_height": "0",
                "tx_finalization_status": "NotFinalized",
                "is_cross_chain_call": false,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "tb1qhamwhl4w4sdv4j6g5vsfntna2a80kvkunllytl",
                    "receiver_chainId": "18333",
                    "coin_type": "Gas",
                    "amount": "900000",
                    "tss_nonce": "130",
                    "gas_limit": "0",
                    "gas_price": "36",
                    "gas_priority_fee": "0",
                    "hash": "ee9f4697a40dc84de2b3410887358e830e66fbd12e44b4a75b2364ae6b1a2ee5",
                    "ballot_index": "0x7d03a4fca71bee0dcd0d2bbce2add42d9921cb3ace4ee7de7e2b18beb528cc28",
                    "observed_external_height": "257083",
                    "gas_used": "0",
                    "effective_gas_price": "0",
                    "effective_gas_limit": "0",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "Executed",
                    "call_options": {
                        "gas_limit": "100",
                        "is_arbitrary_call": true
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V2",
            "revert_options": {
                "revert_address": "0x0000000000000000000000000000000000000000",
                "call_on_revert": false,
                "abort_address": "0x0000000000000000000000000000000000000000",
                "revert_message": null,
                "revert_gas_limit": "0"
            }
        },
        {
            "creator": "zeta1mte0r3jzkf2rkd7ex4p3xsd3fxqg7q29q0wxl5",
            "index": "0xb313d88712a40bcc30b4b7c9aa6f073b9f9eb6e2ae3e4d6e704bd9c15c8a7759",
            "zeta_fees": "0",
            "relayed_message": "",
            "cctx_status": {
                "status": "OutboundPending",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750344267",
                "isAbortRefunded": false,
                "created_timestamp": "1750344267",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "tb1qhamwhl4w4sdv4j6g5vsfntna2a80kvkunllytl",
                "sender_chain_id": "18333",
                "tx_origin": "tb1qhamwhl4w4sdv4j6g5vsfntna2a80kvkunllytl",
                "coin_type": "Gas",
                "asset": "",
                "amount": "998368",
                "observed_hash": "fb9ed1a4f8e3971543f9a598a7b47f70173f5a5f31e43eac2e2fe7911c92254b",
                "observed_external_height": "257081",
                "ballot_index": "0xb313d88712a40bcc30b4b7c9aa6f073b9f9eb6e2ae3e4d6e704bd9c15c8a7759",
                "finalized_zeta_height": "10966666",
                "tx_finalization_status": "Executed",
                "is_cross_chain_call": false,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "0x58A8Ba18c585C411B95Ba1e78962a2A3E1c6f52a",
                    "receiver_chainId": "7001",
                    "coin_type": "Gas",
                    "amount": "0",
                    "tss_nonce": "0",
                    "gas_limit": "0",
                    "gas_price": "",
                    "gas_priority_fee": "",
                    "hash": "0x4b963c94801f5c81b66c3f0e4ceb38fd18832ea43080060a5de6a7b5863f48d8",
                    "ballot_index": "",
                    "observed_external_height": "10966666",
                    "gas_used": "0",
                    "effective_gas_price": "0",
                    "effective_gas_limit": "0",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "NotFinalized",
                    "call_options": {
                        "gas_limit": "0",
                        "is_arbitrary_call": false
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V2",
            "revert_options": {
                "revert_address": "",
                "call_on_revert": false,
                "abort_address": "",
                "revert_message": null,
                "revert_gas_limit": "0"
            }
        }
    ],
    "pagination": {
        "next_key": "////////22c=",
        "total": "0"
    }
}
"#;

pub const PENDING_TX_RESPONSE: &str = r#"
{
            "creator": "zeta1mte0r3jzkf2rkd7ex4p3xsd3fxqg7q29q0wxl5",
            "index": "0xb313d88712a40bcc30b4b7c9aa6f073b9f9eb6e2ae3e4d6e704bd9c15c8a7759",
            "zeta_fees": "0",
            "relayed_message": "",
            "cctx_status": {
                "status": "OutboundPending",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750344267",
                "isAbortRefunded": false,
                "created_timestamp": "1750344267",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "tb1qhamwhl4w4sdv4j6g5vsfntna2a80kvkunllytl",
                "sender_chain_id": "18333",
                "tx_origin": "tb1qhamwhl4w4sdv4j6g5vsfntna2a80kvkunllytl",
                "coin_type": "Gas",
                "asset": "",
                "amount": "998368",
                "observed_hash": "fb9ed1a4f8e3971543f9a598a7b47f70173f5a5f31e43eac2e2fe7911c92254b",
                "observed_external_height": "257081",
                "ballot_index": "0xb313d88712a40bcc30b4b7c9aa6f073b9f9eb6e2ae3e4d6e704bd9c15c8a7759",
                "finalized_zeta_height": "10966666",
                "tx_finalization_status": "Executed",
                "is_cross_chain_call": false,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "0x58A8Ba18c585C411B95Ba1e78962a2A3E1c6f52a",
                    "receiver_chainId": "7001",
                    "coin_type": "Gas",
                    "amount": "0",
                    "tss_nonce": "0",
                    "gas_limit": "0",
                    "gas_price": "",
                    "gas_priority_fee": "",
                    "hash": "0x4b963c94801f5c81b66c3f0e4ceb38fd18832ea43080060a5de6a7b5863f48d8",
                    "ballot_index": "",
                    "observed_external_height": "10966666",
                    "gas_used": "0",
                    "effective_gas_price": "0",
                    "effective_gas_limit": "0",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "NotFinalized",
                    "call_options": {
                        "gas_limit": "0",
                        "is_arbitrary_call": false
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V2",
            "revert_options": {
                "revert_address": "",
                "call_on_revert": false,
                "abort_address": "",
                "revert_message": null,
                "revert_gas_limit": "0"
            }
        }
"#;

pub const FINALIZED_TX_RESPONSE: &str = r#"
{
            "creator": "zeta1mte0r3jzkf2rkd7ex4p3xsd3fxqg7q29q0wxl5",
            "index": "0xb313d88712a40bcc30b4b7c9aa6f073b9f9eb6e2ae3e4d6e704bd9c15c8a7759",
            "zeta_fees": "0",
            "relayed_message": "",
            "cctx_status": {
                "status": "OutboundMined",
                "status_message": "",
                "error_message": "",
                "lastUpdate_timestamp": "1750344267",
                "isAbortRefunded": false,
                "created_timestamp": "1750344267",
                "error_message_revert": "",
                "error_message_abort": ""
            },
            "inbound_params": {
                "sender": "tb1qhamwhl4w4sdv4j6g5vsfntna2a80kvkunllytl",
                "sender_chain_id": "18333",
                "tx_origin": "tb1qhamwhl4w4sdv4j6g5vsfntna2a80kvkunllytl",
                "coin_type": "Gas",
                "asset": "",
                "amount": "998368",
                "observed_hash": "fb9ed1a4f8e3971543f9a598a7b47f70173f5a5f31e43eac2e2fe7911c92254b",
                "observed_external_height": "257081",
                "ballot_index": "0xb313d88712a40bcc30b4b7c9aa6f073b9f9eb6e2ae3e4d6e704bd9c15c8a7759",
                "finalized_zeta_height": "10966666",
                "tx_finalization_status": "Executed",
                "is_cross_chain_call": false,
                "status": "SUCCESS",
                "confirmation_mode": "SAFE"
            },
            "outbound_params": [
                {
                    "receiver": "0x58A8Ba18c585C411B95Ba1e78962a2A3E1c6f52a",
                    "receiver_chainId": "7001",
                    "coin_type": "Gas",
                    "amount": "0",
                    "tss_nonce": "0",
                    "gas_limit": "0", 
                    "gas_price": "",
                    "gas_priority_fee": "",
                    "hash": "0x4b963c94801f5c81b66c3f0e4ceb38fd18832ea43080060a5de6a7b5863f48d8",
                    "ballot_index": "",
                    "observed_external_height": "10966666",
                    "gas_used": "0",
                    "effective_gas_price": "0",
                    "effective_gas_limit": "0",
                    "tss_pubkey": "zetapub1addwnpepq28c57cvcs0a2htsem5zxr6qnlvq9mzhmm76z3jncsnzz32rclangr2g35p",
                    "tx_finalization_status": "Executed",
                    "call_options": {
                        "gas_limit": "0",
                        "is_arbitrary_call": false
                    },
                    "confirmation_mode": "SAFE"
                }
            ],
            "protocol_contract_version": "V2",
            "revert_options": {
                "revert_address": "",
                "call_on_revert": false,
                "abort_address": "",
                "revert_message": null,
                "revert_gas_limit": "0"
            }
        }
"#;

pub fn historic_response_from_file() -> String {
    let file_path = "tests/data/response.json";
    let file_content = fs::read_to_string(file_path).expect("Failed to read file");
    file_content
}