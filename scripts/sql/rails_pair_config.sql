BEGIN TRANSACTION READ ONLY;

SELECT jsonb_pretty(
  jsonb_build_object(
    'id', p.id,
    'updated_at', p.updated_at,
    'active', p.is_active,
    'label', p.label,
    'binance_symbol', p.binance_symbol,
    'binance_step_size', p.binance_step_size::text,
    'binance_tick_size', p.binance_tick_size::text,
    'arbitrage_strategy', CASE p.arbitrage_strategy
      WHEN 0 THEN 'legacy'
      WHEN 1 THEN 'profit_token_a'
      ELSE p.arbitrage_strategy::text
    END,
    'allowed_dex_providers', p.allowed_dex_providers,
    'token_a_min_buy_amount', p.token_a_min_buy_amount::text,
    'token_b_min_buy_amount', p.token_b_min_buy_amount::text,
    'token_a_min_balance', p.token_a_min_balance::text,
    'token_b_min_balance', p.token_b_min_balance::text,
    'token_a_bridge_binance_network_name', p.token_a_bridge_binance_network_name,
    'token_b_bridge_binance_network_name', p.token_b_bridge_binance_network_name,
    'chain', jsonb_build_object(
      'name', c.name,
      'chain_id', c.chain_id,
      'binance_network_name', c.binance_network_name,
      'gas_symbol', c.gas_symbol,
      'gas_decimals', c.gas_decimals,
      'multicall3_address', c.multicall3_address,
      'uniswap_v3_quoter_address', c.uniswap_quoter_address,
      'uniswap_v3_router_address', c.uniswap_v3_router_address,
      'uniswap_v4_quoter_address', c.uniswap_v4_quoter_address,
      'uniswap_v4_router_address', c.uniswap_v4_router_address
    ),
    'token_a', jsonb_build_object(
      'symbol', a.symbol,
      'contract', a.contract,
      'decimals', a.decimals
    ),
    'token_b', jsonb_build_object(
      'symbol', b.symbol,
      'contract', b.contract,
      'decimals', b.decimals
    )
  )
)
FROM trading_pairs p
JOIN tokens a ON a.id = p.token_a_id
JOIN tokens b ON b.id = p.token_b_id
JOIN chains c ON c.id = a.chain_id
WHERE p.binance_symbol = :'binance_symbol'
  AND p.is_active = true
ORDER BY p.id;

COMMIT;
