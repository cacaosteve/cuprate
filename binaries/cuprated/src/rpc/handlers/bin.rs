//! RPC request handler functions (binary endpoints).
//!
//! TODO:
//! Some handlers have `todo!()`s for other Cuprate internals that must be completed, see:
//! <https://github.com/Cuprate/cuprate/pull/355>

use std::num::NonZero;

use anyhow::{anyhow, Error};
use bytes::Bytes;
use monero_oxide::{
    block::Block,
    primitives::keccak256,
    transaction::{NotPruned, Transaction},
};
use cuprate_blockchain::service::BlockchainReadHandle;

use cuprate_constants::rpc::{RESTRICTED_BLOCK_COUNT, RESTRICTED_TRANSACTIONS_COUNT};
use cuprate_fixed_bytes::ByteArrayVec;
use cuprate_helper::cast::{u64_to_usize, usize_to_u64};
use cuprate_rpc_interface::RpcHandler;
use cuprate_rpc_types::{
    base::{AccessResponseBase, ResponseBase},
    bin::{
        BinRequest, BinResponse, GetBlocksByHeightRequest, GetBlocksByHeightResponse,
        GetBlocksRequest, GetBlocksResponse, GetHashesRequest, GetHashesResponse,
        GetOutputIndexesRequest, GetOutputIndexesResponse, GetOutsRequest, GetOutsResponse,
        GetTransactionPoolHashesRequest, GetTransactionPoolHashesResponse,
    },
    json::{GetOutputDistributionRequest, GetOutputDistributionResponse},
    misc::RequestedInfo,
};
use cuprate_types::{
    rpc::{BlockOutputIndices, PoolInfo, PoolInfoExtent, TxOutputIndices},
    BlockCompleteEntry, PrunedTxBlobEntry, TransactionBlobs,
};

use crate::rpc::{
    handlers::{helper, shared, shared::not_available},
    service::{blockchain, txpool},
    CupratedRpcHandler,
};

/// Map a [`BinRequest`] to the function that will lead to a [`BinResponse`].
pub async fn map_request(
    state: CupratedRpcHandler,
    request: BinRequest,
) -> Result<BinResponse, Error> {
    use BinRequest as Req;
    use BinResponse as Resp;

    Ok(match request {
        Req::GetBlocks(r) => Resp::GetBlocks(get_blocks(state, r).await?),
        Req::GetBlocksByHeight(r) => Resp::GetBlocksByHeight(not_available()?),
        Req::GetHashes(r) => Resp::GetHashes(get_hashes(state, r).await?),
        Req::GetOutputIndexes(r) => Resp::GetOutputIndexes(not_available()?),
        Req::GetOuts(r) => Resp::GetOuts(get_outs(state, r).await?),
        Req::GetTransactionPoolHashes(r) => {
            Resp::GetTransactionPoolHashes(get_transaction_pool_hashes(state, r).await?)
        }
        Req::GetOutputDistribution(r) => Resp::GetOutputDistribution(not_available()?),
    })
}

/// <https://github.com/monero-project/monero/blob/cc73fe71162d564ffda8e549b79a350bca53c454/src/rpc/core_rpc_server.cpp#L611-L789>
async fn get_blocks(
    mut state: CupratedRpcHandler,
    request: GetBlocksRequest,
) -> Result<GetBlocksResponse, Error> {
    // Time should be set early:
    // <https://github.com/monero-project/monero/blob/cc73fe71162d564ffda8e549b79a350bca53c454/src/rpc/core_rpc_server.cpp#L628-L631>
    let daemon_time = cuprate_helper::time::current_unix_timestamp();

    let GetBlocksRequest {
        requested_info,
        block_ids,
        start_height,
        prune,
        no_miner_tx,
        pool_info_since,
        max_block_count,
    } = request;

    let block_hashes: Vec<[u8; 32]> = (&block_ids).into();
    drop(block_ids);

    let (get_blocks, get_pool) = match requested_info {
        RequestedInfo::BlocksOnly => (true, false),
        RequestedInfo::BlocksAndPool => (true, true),
        RequestedInfo::PoolOnly => (false, true),
    };

    let pool_info_extent = PoolInfoExtent::None;

    let pool_info = if get_pool {
        let is_restricted = state.is_restricted();
        let include_sensitive_txs = !is_restricted;

        let max_tx_count = if is_restricted {
            RESTRICTED_TRANSACTIONS_COUNT
        } else {
            usize::MAX
        };

        txpool::pool_info(
            &mut state.txpool_read,
            include_sensitive_txs,
            max_tx_count,
            NonZero::new(u64_to_usize(request.pool_info_since)),
        )
        .await?
    } else {
        PoolInfo::None
    };

    let resp = GetBlocksResponse {
        base: helper::access_response_base(false),
        blocks: vec![],
        start_height: 0,
        current_height: 0,
        output_indices: vec![],
        daemon_time,
        pool_info,
    };

    if !get_blocks {
        return Ok(resp);
    }

    if block_hashes.is_empty() && max_block_count != 0 {
        let (height, _) = helper::top_height(&mut state).await?;
        let current_height = u64_to_usize(height + 1);
        let start_height_usize = u64_to_usize(start_height);
        let max_block_count_usize = u64_to_usize(max_block_count);
        let end_height = start_height_usize
            .saturating_add(max_block_count_usize)
            .min(current_height);

        let heights: Vec<u64> = (start_height_usize..end_height).map(usize_to_u64).collect();
        let blocks =
            blockchain::block_complete_entries_by_height(&mut state.blockchain_read, heights).await?;
        let blocks = wallet_compatible_block_entries(blocks)?;
        let output_indices =
            block_output_indices_for_entries(&mut state.blockchain_read, &blocks, no_miner_tx).await?;

        return Ok(GetBlocksResponse {
            blocks,
            start_height,
            current_height: usize_to_u64(current_height),
            output_indices,
            ..resp
        });
    }

    if let Some(block_id) = block_hashes.first() {
        let (height, hash) = helper::top_height(&mut state).await?;

        if hash == *block_id {
            return Ok(GetBlocksResponse {
                current_height: height + 1,
                ..resp
            });
        }
    }

    let (block_hashes, start_height, _) =
        blockchain::next_chain_entry(&mut state.blockchain_read, block_hashes, start_height)
            .await?;

    if start_height.is_none() {
        return Err(anyhow!("Block IDs were not sorted properly"));
    }

    let (blocks, missing_hashes, height) =
        blockchain::block_complete_entries(&mut state.blockchain_read, block_hashes).await?;

    if !missing_hashes.is_empty() {
        return Err(anyhow!("Missing blocks"));
    }

    Ok(GetBlocksResponse {
        blocks,
        current_height: usize_to_u64(height),
        ..resp
    })
}

fn wallet_compatible_block_entries(
    blocks: Vec<BlockCompleteEntry>,
) -> Result<Vec<BlockCompleteEntry>, Error> {
    blocks
        .into_iter()
        .map(|mut entry| {
            if let Some(normal_txs) = entry.txs.clone().take_normal() {
                let mut pruned_txs = Vec::with_capacity(normal_txs.len());
                for tx_blob in normal_txs {
                    let mut tx_bytes = tx_blob.as_ref();
                    let tx: Transaction<NotPruned> = Transaction::read(&mut tx_bytes)?;
                    if !tx_bytes.is_empty() {
                        return Err(anyhow!("Transaction blob had extraneous bytes after parse"));
                    }
                    pruned_txs.push(PrunedTxBlobEntry {
                        blob: tx_blob,
                        prunable_hash: tx_prunable_hash(&tx).into(),
                    });
                }
                entry.txs = TransactionBlobs::Pruned(pruned_txs);
            }
            Ok(entry)
        })
        .collect()
}

fn tx_prunable_hash(tx: &Transaction<NotPruned>) -> [u8; 32] {
    match tx {
        Transaction::V1 { .. } => [0; 32],
        Transaction::V2 { proofs, .. } => {
            if let Some(proofs) = proofs {
                let mut buf = Vec::with_capacity(1024);
                proofs
                    .prunable
                    .write(&mut buf, proofs.rct_type())
                    .expect("write failed but Vec doesn't fail");
                keccak256(buf)
            } else {
                [0; 32]
            }
        }
    }
}

async fn block_output_indices_for_entries(
    blockchain_read: &mut BlockchainReadHandle,
    blocks: &[BlockCompleteEntry],
    no_miner_tx: bool,
) -> Result<Vec<BlockOutputIndices>, Error> {
    let mut all_block_output_indices = Vec::with_capacity(blocks.len());

    for entry in blocks {
        let mut block_blob = entry.block.as_ref();
        let block = Block::read(&mut block_blob)?;

        let mut tx_output_indices = Vec::with_capacity(block.transactions.len() + usize::from(!no_miner_tx));

        if !no_miner_tx {
            tx_output_indices.push(TxOutputIndices {
                indices: blockchain::tx_output_indexes(blockchain_read, block.miner_transaction.hash())
                    .await?,
            });
        }

        for tx_hash in &block.transactions {
            tx_output_indices.push(TxOutputIndices {
                indices: blockchain::tx_output_indexes(blockchain_read, *tx_hash).await?,
            });
        }

        // Sanity check: if tx blobs are present, they should match tx hashes in the block blob.
        if let Some(normal_txs) = entry.txs.clone().take_normal() {
            if normal_txs.len() != block.transactions.len() {
                return Err(anyhow!("Block tx hash count mismatched tx blob count"));
            }

            for (tx_blob, expected_hash) in normal_txs.into_iter().zip(&block.transactions) {
                let mut tx_blob = tx_blob.as_ref();
                let tx = Transaction::read(&mut tx_blob)?;
                if tx.hash() != *expected_hash {
                    return Err(anyhow!("Block tx blob hash mismatched tx hash in block"));
                }
            }
        }

        all_block_output_indices.push(BlockOutputIndices {
            indices: tx_output_indices,
        });
    }

    Ok(all_block_output_indices)
}

/// <https://github.com/monero-project/monero/blob/cc73fe71162d564ffda8e549b79a350bca53c454/src/rpc/core_rpc_server.cpp#L817-L857>
async fn get_blocks_by_height(
    mut state: CupratedRpcHandler,
    request: GetBlocksByHeightRequest,
) -> Result<GetBlocksByHeightResponse, Error> {
    if state.is_restricted() && request.heights.len() > RESTRICTED_BLOCK_COUNT {
        return Err(anyhow!("Too many blocks requested in restricted mode"));
    }

    let blocks =
        blockchain::block_complete_entries_by_height(&mut state.blockchain_read, request.heights)
            .await?;

    Ok(GetBlocksByHeightResponse {
        base: helper::access_response_base(false),
        blocks,
    })
}

/// <https://github.com/monero-project/monero/blob/cc73fe71162d564ffda8e549b79a350bca53c454/src/rpc/core_rpc_server.cpp#L859-L880>
async fn get_hashes(
    mut state: CupratedRpcHandler,
    request: GetHashesRequest,
) -> Result<GetHashesResponse, Error> {
    let GetHashesRequest {
        start_height,
        block_ids,
    } = request;

    // FIXME: impl `last()`
    let last = {
        let len = block_ids.len();

        if len == 0 {
            return Err(anyhow!("block_ids empty"));
        }

        block_ids[len - 1]
    };

    let hashes: Vec<[u8; 32]> = (&block_ids).into();

    let (m_blocks_ids, _, current_height) =
        blockchain::next_chain_entry(&mut state.blockchain_read, hashes, start_height).await?;

    Ok(GetHashesResponse {
        base: helper::access_response_base(false),
        m_blocks_ids: m_blocks_ids.into(),
        current_height: usize_to_u64(current_height),
        start_height,
    })
}

/// <https://github.com/monero-project/monero/blob/cc73fe71162d564ffda8e549b79a350bca53c454/src/rpc/core_rpc_server.cpp#L959-L977>
async fn get_output_indexes(
    mut state: CupratedRpcHandler,
    request: GetOutputIndexesRequest,
) -> Result<GetOutputIndexesResponse, Error> {
    Ok(GetOutputIndexesResponse {
        base: helper::access_response_base(false),
        o_indexes: blockchain::tx_output_indexes(&mut state.blockchain_read, request.txid).await?,
    })
}

/// <https://github.com/monero-project/monero/blob/cc73fe71162d564ffda8e549b79a350bca53c454/src/rpc/core_rpc_server.cpp#L882-L910>
async fn get_outs(
    state: CupratedRpcHandler,
    request: GetOutsRequest,
) -> Result<GetOutsResponse, Error> {
    shared::get_outs(state, request).await
}

/// <https://github.com/monero-project/monero/blob/cc73fe71162d564ffda8e549b79a350bca53c454/src/rpc/core_rpc_server.cpp#L1689-L1711>
async fn get_transaction_pool_hashes(
    mut state: CupratedRpcHandler,
    _: GetTransactionPoolHashesRequest,
) -> Result<GetTransactionPoolHashesResponse, Error> {
    Ok(GetTransactionPoolHashesResponse {
        base: helper::access_response_base(false),
        tx_hashes: shared::get_transaction_pool_hashes(state)
            .await
            .map(ByteArrayVec::from)?,
    })
}

/// <https://github.com/monero-project/monero/blob/cc73fe71162d564ffda8e549b79a350bca53c454/src/rpc/core_rpc_server.cpp#L3352-L3398>
async fn get_output_distribution(
    state: CupratedRpcHandler,
    request: GetOutputDistributionRequest,
) -> Result<GetOutputDistributionResponse, Error> {
    shared::get_output_distribution(state, request).await
}
