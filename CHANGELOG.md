# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.0.0 (2026-03-17)


### Features

* **admin-ui:** add schema, health, and retention tabs to console ([95648d1](https://github.com/josedab/chronicle/commit/95648d1e2ff2ff630ef1274625cd3edde835f260))
* **agent:** add zero-code observability agent with eBPF collection ([c42c82e](https://github.com/josedab/chronicle/commit/c42c82e497585cb89d05875100d06ba52b8434cd))
* **anomaly:** add causal explainer with Shapley and Granger analysis ([657ea61](https://github.com/josedab/chronicle/commit/657ea61bf8fc36624040d924ad71e4cca5fd9e43))
* **anomaly:** add DBSCAN, isolation forest, and online learning ([ad0e5db](https://github.com/josedab/chronicle/commit/ad0e5db47058ccd800b4d02086d715d7a991f782))
* **audit_log:** add file-based persistence with automatic size rotation ([6031798](https://github.com/josedab/chronicle/commit/6031798331532159a51ac774b8f0562019370750))
* **audit:** add hash-linked audit trail with SIEM integration ([55490f8](https://github.com/josedab/chronicle/commit/55490f847799e087afe527f3e12677e5688659e5))
* **backup:** add point-in-time recovery with deduplication ([85b9801](https://github.com/josedab/chronicle/commit/85b9801771503fcc795c38c573dec4e7d1b41abc))
* **backup:** implement real data persistence and checksums in incremental backup ([54dca81](https://github.com/josedab/chronicle/commit/54dca811181d2eee449e16cf0f943dbf7429a684))
* **branching:** add persistent branch storage with snapshots ([d8b4c4e](https://github.com/josedab/chronicle/commit/d8b4c4e42d66d9bb89b01f02071a0845292ec403))
* **cache:** add canonical QueryCacheKey with stable tag and filter ordering ([f1b457f](https://github.com/josedab/chronicle/commit/f1b457f880b07c6661900038a5073137f633f606))
* **cardinality:** add adaptive HyperLogLog cardinality estimator ([0f058cf](https://github.com/josedab/chronicle/commit/0f058cfb0edd9b0e1bb15baef00078dae493599a))
* **cardinality:** add histogram merging, adaptive rebuild, and CBO feedback ([bb28ee8](https://github.com/josedab/chronicle/commit/bb28ee8a7f04bc98f8aeb736bd705347174a9899))
* **cbo:** add columnar batch plan to cost-based optimizer ([9fd94b7](https://github.com/josedab/chronicle/commit/9fd94b7a0b2a315d9455ae9aad93d9282dab3af8))
* **chaos:** add context-aware delay to SlowIOFault ([fa5e394](https://github.com/josedab/chronicle/commit/fa5e39474490709ebafd23dc725209eb16096bc2))
* **chaos:** add pluggable fault injectors for resilience testing ([288db3d](https://github.com/josedab/chronicle/commit/288db3da612f844f60cfa8af35eecb800836b70d))
* **chaos:** add resilience scenario runner with assertions ([299eb94](https://github.com/josedab/chronicle/commit/299eb94d31f3c4d963ee0a7c2ed082a1f743e391))
* **ci:** add file-size check step to CI pipeline ([f4860d4](https://github.com/josedab/chronicle/commit/f4860d48ee1a2b79650e2c4eb1fe902ba76d3ded))
* **ci:** auto-label first-time contributor PRs ([025aa09](https://github.com/josedab/chronicle/commit/025aa095936acad7c8d78209353e83a40660d26f))
* **cluster:** add pre-vote protocol and linearizable read support ([f7ddb3a](https://github.com/josedab/chronicle/commit/f7ddb3a7645c6bb6c6c72152484dc521ad98b86f))
* **codec:** add adaptive codec selector with data pattern analysis ([a4e7e4e](https://github.com/josedab/chronicle/commit/a4e7e4edd1bcd33ec06608f100399cd076d23cff))
* **collector:** add chronicle-collector standalone binary ([f6aee0c](https://github.com/josedab/chronicle/commit/f6aee0ca21750c1080bdf8d1301447db8edbb60d))
* **columnar:** add batch stats, zone-map pruning, and column projection ([fd2ef59](https://github.com/josedab/chronicle/commit/fd2ef59263e2eb9d9e0e52773949b425976d48ec))
* **columnar:** add vectorized columnar batch processing engine ([b7fe9ba](https://github.com/josedab/chronicle/commit/b7fe9ba0864751fcc16b6ed6ee26f4a6845d8f0b))
* **compat:** add migration bridge package for module split ([2feb79d](https://github.com/josedab/chronicle/commit/2feb79ddce336231ae99b0027fde02df17b757b6))
* **compression:** add pluggable codec registry and auto-profiling pipeline ([4a0b19b](https://github.com/josedab/chronicle/commit/4a0b19b13cb9795f21528ebbb8f27396af4f9ea8))
* **confidential:** add hardware TEE attestation and secure enclave ([1a32349](https://github.com/josedab/chronicle/commit/1a32349be1349e5cb6fb85fd509b755f36db62ea))
* **config:** add file-based hot reload with auto-apply of safe fields ([6d1a9f0](https://github.com/josedab/chronicle/commit/6d1a9f056a10e9d76719f365824525a700d58f6e))
* **config:** warn on plaintext API keys during validation ([5e0f18a](https://github.com/josedab/chronicle/commit/5e0f18a6d9404ea199c1bc72ed4b6e64113d7199))
* **connector_hub:** add file sink driver for persistent JSON-lines export ([1b3e6ac](https://github.com/josedab/chronicle/commit/1b3e6ac5546de1d7666644d7da98e96e6409f1e0))
* **continuous-agg:** add SQL management API with alter and delete ([b360500](https://github.com/josedab/chronicle/commit/b36050057abe9822c59877a3297ba6a8269a7a22))
* **continuous-agg:** add watermarks, dedup, and checkpoint/restore ([3bdc9d7](https://github.com/josedab/chronicle/commit/3bdc9d77013859a843ef2068f6528a272543a409))
* **correlation:** add cross-signal correlation engine ([fa665e0](https://github.com/josedab/chronicle/commit/fa665e090be19ef7696e850946eb8e1c3d3364df))
* **correlation:** add trace ID index, time-range correlation, and cardinality limits ([74c70ba](https://github.com/josedab/chronicle/commit/74c70ba2694a48a5157e5d267fa9f413f5f9be3f))
* **data_contracts,schema:** add freshness, cardinality, not-null rules and type validation ([794074f](https://github.com/josedab/chronicle/commit/794074f5bd343a9b35852d7dc4e094f6e0eaa405))
* **data_masking:** upgrade hash masking action from FNV to HMAC-SHA256 ([f1906df](https://github.com/josedab/chronicle/commit/f1906dfd5b0fbea0bd445b800f51591c84860c0a))
* **db:** add DeleteMetric and Compact public API ([27a918f](https://github.com/josedab/chronicle/commit/27a918fc2fb858da58b03f7a5946dbf478027466))
* **dedup,retention,streaming,profile:** lifecycle management and background cleanup ([96bf48c](https://github.com/josedab/chronicle/commit/96bf48c94eb82823eaaf3fdaf09377f77857ac31))
* **deprecation:** track and warn on deprecated config field usage ([091eeb5](https://github.com/josedab/chronicle/commit/091eeb5afaf0e3534aae160a37e225562fa9fdc0))
* **devcontainer:** enhance dev container with extensions and ports ([5a1939c](https://github.com/josedab/chronicle/commit/5a1939c0545cbb5f96b1195df1d0ef5939a65412))
* **duckdb:** add virtual tables, predicate pushdown, and Parquet export ([fa858a4](https://github.com/josedab/chronicle/commit/fa858a4479c6342ee5e7ad3c8d7dfc7cb0803d6c))
* **ebpf:** add cgroup-aware container metrics with pod enrichment ([57952f8](https://github.com/josedab/chronicle/commit/57952f81da66ae7562e6a8a0bd258e752722ecaf))
* **ebpf:** add HTTP request tracing with latency histograms ([42af990](https://github.com/josedab/chronicle/commit/42af990659c22700f232dfce0fd7a338ab61ec02))
* **ebpf:** add kernel probe and tracepoint instrumentation ([8d8d1a1](https://github.com/josedab/chronicle/commit/8d8d1a1c66f8194feb97ac377ed70cef89d265a3))
* **ebpf:** add network connection and DNS monitoring collector ([ca21576](https://github.com/josedab/chronicle/commit/ca21576f51a8341d5de1636d288eeab47b39b88b))
* **ebpf:** add TCP connection tracking agent with kprobe stubs ([3f16178](https://github.com/josedab/chronicle/commit/3f161785ec24639f5482c3636f4cf57c71d950ad))
* **edge:** add federated query engine with bloom filter routing ([822a470](https://github.com/josedab/chronicle/commit/822a470b211ac91c9dcb24a7865f03e9111fb0aa))
* **embedded-cluster:** add leader lease, anti-entropy, and simulation ([2487bab](https://github.com/josedab/chronicle/commit/2487bab9b3f33b3e80eabfca750e5b04d9a61ede))
* **encoding,codec:** add OOM guards in dictionary encoding and partition decoding ([00e34e9](https://github.com/josedab/chronicle/commit/00e34e9c565c4085bc1ce658b87337f3a42312ae))
* **examples:** add IoT gateway example with AWS S3 integration ([51e3b4f](https://github.com/josedab/chronicle/commit/51e3b4f5f7b9bc7ea368fc6643d9fa9a0d1a5b4b))
* **export:** add Parquet/Iceberg export engine ([69fa566](https://github.com/josedab/chronicle/commit/69fa56656d0243e2faddc9ab9cfdab73b339537d))
* **ffi:** add JSON-based query and write APIs for language bindings ([9a75269](https://github.com/josedab/chronicle/commit/9a7526935c43670c28fda41766914bcccf390b2a))
* **flags:** add CheckFeature guard and wrap next-gen routes with feature gate ([4ca12c5](https://github.com/josedab/chronicle/commit/4ca12c539d67f750bac8c951c026ff0421342726))
* **fleet:** add orchestration with config push and scatter-gather ([81a1769](https://github.com/josedab/chronicle/commit/81a176979fa3c8d29324d74576c7913db1efcffb))
* **flight-sql:** add standard RPC methods for client interop ([a9679ed](https://github.com/josedab/chronicle/commit/a9679ed8cefab848841de9227b90e40eebd7faff))
* **flight-sql:** add transaction management support ([e9ff58c](https://github.com/josedab/chronicle/commit/e9ff58c5a6d6c04d236ea1eb610f05c131156a58))
* **forecast:** replace linear regression with trend-seasonal decomposition ([14afab3](https://github.com/josedab/chronicle/commit/14afab30ad559574957e7d4b4f700600204333d6))
* **grpc:** add native protobuf wire format encoder/decoder ([b912640](https://github.com/josedab/chronicle/commit/b9126409f5ccbf1349f4029e4b631103c9cf4d1c))
* **grpc:** add OTLP MetricsService with proto wire decoding ([c03c545](https://github.com/josedab/chronicle/commit/c03c5450ab7618ba3ccb1ef685bcfa4fb8578b3a))
* **health_check:** add memory and disk space component checkers ([87d9502](https://github.com/josedab/chronicle/commit/87d9502b8f024d31b732ca2d64c064d452bcb00a))
* **health:** add write/query latency probes and WAL size threshold ([fb4bd14](https://github.com/josedab/chronicle/commit/fb4bd148bf2e86d0556c8c5b6c31165484e208d2))
* **histogram:** add native exponential histogram support ([b3d4e91](https://github.com/josedab/chronicle/commit/b3d4e9192bd036510c59ff6fee4240bf4d91d20c))
* **http:** add CORS middleware with per-origin allowlist support ([9e4b1c6](https://github.com/josedab/chronicle/commit/9e4b1c678fe891b59a7c9ae010a90912a46a9e8a))
* **http:** add request ID middleware for X-Request-ID propagation ([9075ca2](https://github.com/josedab/chronicle/commit/9075ca22bbc1598cf9d3f8a08008c3a9d8e029ac))
* **http:** add security headers middleware ([8ae7772](https://github.com/josedab/chronicle/commit/8ae7772414adbaf5f034c0a099a59cbac69ecfb4))
* **httputil:** add internal error helper package ([ecc7f70](https://github.com/josedab/chronicle/commit/ecc7f70e9439ec29af743924453e9b03557132a4))
* **http:** wire CORS and metrics middleware and expose /api/v1/http/metrics ([aed4573](https://github.com/josedab/chronicle/commit/aed45737b9de62320401938510c838b90f24db3a))
* **k8s:** add PDB, canary upgrades, network policy, and dashboard ConfigMap ([ba7a900](https://github.com/josedab/chronicle/commit/ba7a90064c21973ffd02a7fff4b4012b16c7f657))
* **lifecycle:** add automated metric state machine transitions ([5220d64](https://github.com/josedab/chronicle/commit/5220d6470c713e1c900a4f7e495f508d99543efe))
* **lifecycle:** add intelligent data lifecycle engine ([15f9ad5](https://github.com/josedab/chronicle/commit/15f9ad5f60e0117b8d945c18847994dbce3b293f))
* **llm:** add OpenAI-compatible LLM provider with routing ([bb9a172](https://github.com/josedab/chronicle/commit/bb9a172239f001aaf2cf626f26e73c1a6e58b40c))
* **logging:** add debug tracing to core operations ([66f3b15](https://github.com/josedab/chronicle/commit/66f3b15ccbcf8e94799c1858429281a32962c985))
* **logs:** add structured log store with severity levels and correlation ([d8725d9](https://github.com/josedab/chronicle/commit/d8725d91ea0907d4c2280baa80223e844378bd0b))
* **makefile:** add developer workflow and validation targets ([a70cd8e](https://github.com/josedab/chronicle/commit/a70cd8e33eb347e450357e69c0c1106022a02f38))
* **makefile:** add per-package coverage and code generation targets ([dc53ece](https://github.com/josedab/chronicle/commit/dc53ecea43fcaad06b32070bff7bedba39374c0f))
* **makefile:** add validate and lint-fast targets ([323b3c9](https://github.com/josedab/chronicle/commit/323b3c9c6a499072a687eef1ae7172d8462ce366))
* **makefile:** enhance quickstart with build and smoke tests ([b6224d4](https://github.com/josedab/chronicle/commit/b6224d4097f126ac6046e52c4dfa79a11e4a2a3d))
* **matviews:** add staleness tracking and schema invalidation ([7d4baf9](https://github.com/josedab/chronicle/commit/7d4baf9599ee8b1f5801a56af58cab7225cebd23))
* **metering:** add per-tenant usage metering and billing webhooks ([748c876](https://github.com/josedab/chronicle/commit/748c8761dad5957739ef6237126a96dfb39ca15d))
* **notebook:** add persistent notebook workbench with collaborative editing ([0631a79](https://github.com/josedab/chronicle/commit/0631a79438b974e9fdd97afbaf1b725be7f49f0f))
* **observability:** real autoscale metrics, health checks, CQ stats, conn pool health loop ([39f85cc](https://github.com/josedab/chronicle/commit/39f85cc2983b5f2ca6c0735dd81bbb1c07b3a811))
* **openapi:** document missing endpoints and expand core/query engine tests ([cabaf59](https://github.com/josedab/chronicle/commit/cabaf59d2f3e40df27fab3b915e529e8d14c0321))
* **otel:** add custom OTel collector distribution with hot reload ([c196b66](https://github.com/josedab/chronicle/commit/c196b66bd182a0f4c19305a49608a54432fb5bff))
* **otel:** add StatsD parser, pipeline validation, and K8s manifests ([3184a5f](https://github.com/josedab/chronicle/commit/3184a5f9b948d9069964437ff322d62271fa2031))
* **otlp:** add native protobuf decoding and snappy decompression ([f26f5a1](https://github.com/josedab/chronicle/commit/f26f5a1cd1c96035320f0f224fdfcbfaf10dedc2))
* **partition:** add bloom filter-based partition pruning ([ecfb5c6](https://github.com/josedab/chronicle/commit/ecfb5c6a4078184889c1021a262f304304dadf7b))
* **partition:** add hash, range, and hybrid partitioning strategies ([7f72f08](https://github.com/josedab/chronicle/commit/7f72f0878fdf67c8d209fcd712d5d483f43396cd))
* **partition:** integrate bloom filter into partition core ([2b9fdf1](https://github.com/josedab/chronicle/commit/2b9fdf175b49dcc8413f015d8f2974b26b3c2f12))
* **perf:** add performance regression detection engine ([a62631f](https://github.com/josedab/chronicle/commit/a62631f99c0e92f3ffce9adc67830ac2d3902a23))
* **pgwire:** add batch COPY writes, parameter binding, and Close handler ([aa7a976](https://github.com/josedab/chronicle/commit/aa7a97671a38574f31ea92d6af93fdb270c36c8a))
* **pgwire:** add MD5 and SCRAM-SHA-256 auth with COPY IN protocol ([d1747ea](https://github.com/josedab/chronicle/commit/d1747ea745763a5e1d4191a3fd6c0ee50b49d248))
* **pgwire:** implement extended query protocol and catalog queries ([f21dc18](https://github.com/josedab/chronicle/commit/f21dc181f1a8820b0cbb33c8614af899258e1aca))
* **pgwire:** wire COPY IN handler into session statement dispatcher ([d4a5ef2](https://github.com/josedab/chronicle/commit/d4a5ef251286d3ed12e1c1a83fc8022bc4d42efb))
* **plugin-sdk:** add capabilities, validation, and dependency resolution ([b389520](https://github.com/josedab/chronicle/commit/b3895205f8728b26c25bcaf257d091a584407681))
* **privacy:** add Rényi DP accountant for tight composition bounds ([2ac7183](https://github.com/josedab/chronicle/commit/2ac7183a9b48ff260281bc9eb197e9fb15485ff2))
* **privacy:** wire Rényi DP accountant into privacy federation ([abae802](https://github.com/josedab/chronicle/commit/abae80226cde97aac2ca243114482167cc0b86ed))
* **profiling:** add enhanced profiling with flame graph diff and HTTP API ([b0b7230](https://github.com/josedab/chronicle/commit/b0b7230ab8f6c942c9e8257d5793b960f6953d8a))
* **profiling:** add Pyroscope-compatible continuous profiling backend ([6815faa](https://github.com/josedab/chronicle/commit/6815faadf90c2ce8bdfdccf8b582171752739e32))
* **profiling:** integrate partition store into continuous profiling engine ([73e4108](https://github.com/josedab/chronicle/commit/73e4108f997a30f12a160759567389dae95a2273))
* **promql:** add holt-winters and native histogram functions ([bec434d](https://github.com/josedab/chronicle/commit/bec434d23a7113824a253338831babd27704bc73))
* **promql:** add parser and evaluator integration for new functions ([dbf2dd3](https://github.com/josedab/chronicle/commit/dbf2dd36606794a49ca112b37b35b9909510c1f3))
* **promql:** add predict_linear/last_over_time and improve binary ops ([64411bf](https://github.com/josedab/chronicle/commit/64411bf2e709e0f9f7e3d8b27213bfbeff6539f2))
* **promql:** add sgn, clamp, histogram, and subquery functions ([4f5a256](https://github.com/josedab/chronicle/commit/4f5a256ae122eafd025be72a67ba04e38014d252))
* **promql:** extend aggregation operators and range functions ([b58adf0](https://github.com/josedab/chronicle/commit/b58adf0a8cd7fc9cb5a74a0d4d8fea65446d7408))
* **query_engine,instrumentation:** add timeout config and wire write/query metrics to SLO engine ([62ca549](https://github.com/josedab/chronicle/commit/62ca5498b1d99440c8557147d465cb9c30adfb64))
* **query-engine:** add predicate pushdown and adaptive execution ([456cdee](https://github.com/josedab/chronicle/commit/456cdee85696e0e7aa5d29ccec4662b3b649a958))
* **query:** add Arrow Flight SQL protocol support ([d8e809a](https://github.com/josedab/chronicle/commit/d8e809a7cc97eca4abf7209d1ef041f39112ebae))
* **query:** add configurable percentile to Aggregation struct ([6cade10](https://github.com/josedab/chronicle/commit/6cade10bda12d69ac9e36cc04238b0900ce9486a))
* **query:** add CORRELATE query language for cross-signal queries ([60c1829](https://github.com/josedab/chronicle/commit/60c1829816c414734629560a4960512905272340))
* **query:** add cost-based query optimizer ([eab433b](https://github.com/josedab/chronicle/commit/eab433bb8a055a64e5b52879bc6de83bca6956fa))
* **query:** add distributed query federation engine ([835cf8a](https://github.com/josedab/chronicle/commit/835cf8a5d90093ab6986548438a7ef3b78c264ac))
* **query:** add UDF aggregation and vectorized execution path ([853485b](https://github.com/josedab/chronicle/commit/853485b82be934fd167cad4f6e9b2964fc10dd4e))
* **query:** integrate CBO-driven adaptive execution path ([c3bd0bd](https://github.com/josedab/chronicle/commit/c3bd0bd3469cd61bb23974597144faeb862e8ca6))
* **raft:** add deterministic network simulation for testing ([418a14f](https://github.com/josedab/chronicle/commit/418a14f2eaa26fa1eaac964fd7cba35842191885))
* **recording_rules:** limit concurrent evaluation with concurrency semaphore ([1aa132b](https://github.com/josedab/chronicle/commit/1aa132b41e54ec0d97dcc52b0399268d7ea2d9f4))
* **relabeling:** add pre-write metric relabeling pipeline ([6bfc6d5](https://github.com/josedab/chronicle/commit/6bfc6d586342f871d3d87ea81ec4cf007eda1bd8))
* **replication:** add dead-letter queue with bounded overflow ([cb06399](https://github.com/josedab/chronicle/commit/cb0639901522c6bad1820183be3bb797e56767f5))
* **s3:** add eventual consistency handling and backend status ([df985bc](https://github.com/josedab/chronicle/commit/df985bca1d9050feb5517523e12babfdb79e22fc))
* **saas:** add SSO federation and tenant quota management ([870ef5c](https://github.com/josedab/chronicle/commit/870ef5ca9ba6bcbdaa80acc50aa375e7261b66ae))
* **scenario:** add what-if scenario engine with data transformations ([dcbc5c3](https://github.com/josedab/chronicle/commit/dcbc5c3a936bcdf6cf94a70b1c73a0b4dce1bb45))
* **schema:** add compatibility checks and schema change audit log ([5da9e57](https://github.com/josedab/chronicle/commit/5da9e57bef37e0a00ee85a62a7b3ceb3f55361df))
* **schema:** add schema compatibility checker with versioned evolution ([c5433ee](https://github.com/josedab/chronicle/commit/c5433ee49fce6653421b74aa7db278555b584bb4))
* **sdk:** add Java SDK with JNA bindings ([8317e33](https://github.com/josedab/chronicle/commit/8317e33292db28a9992e22c5de7171bb1883b98a))
* **sdk:** add Node.js SDK with CLI process wrapper ([8d20c6e](https://github.com/josedab/chronicle/commit/8d20c6e5ee9026ca57554d8a263a939034457f1b))
* **sdk:** add Python SDK with ctypes FFI and pandas integration ([5bf5954](https://github.com/josedab/chronicle/commit/5bf59541a80be5dbdfc870404133a5ec6325f856))
* **sdk:** add Rust SDK with safe FFI wrappers ([d65e1ae](https://github.com/josedab/chronicle/commit/d65e1ae1b716955f4e2fe30305b8a62986aa4159))
* **self-instrumentation:** add latency histograms and writesRejected counter ([a85623f](https://github.com/josedab/chronicle/commit/a85623f5cda0f00984a529fd992542cea2db7b2e))
* **slo:** add configurable burn rate thresholds and synchronous Stop ([20971ee](https://github.com/josedab/chronicle/commit/20971ee4255ea5a5dcdaba8633c5aae3ac1d9170))
* **slo:** add SLO engine with multi-window burn rate alerting ([7862db6](https://github.com/josedab/chronicle/commit/7862db6424624692e6cd43916307b7413b3c2da1))
* **stream:** add timeout to operator processing ([34e04c7](https://github.com/josedab/chronicle/commit/34e04c7b7d1a2768717c60a44a0a8c5089d9a98a))
* **streaming:** add SQL:2016 streaming engine v2 ([3fe8d29](https://github.com/josedab/chronicle/commit/3fe8d2976b526ea3b6f8961cfa70e16e3a687aa6))
* **streaming:** make trigger and expiry intervals configurable ([9b65304](https://github.com/josedab/chronicle/commit/9b653049aaa9c40f26d48fa79c9247753b6abd3e))
* **tenant:** add tenant governance and chargeback engine ([2fde4a4](https://github.com/josedab/chronicle/commit/2fde4a4e4b3c5c45a53ac15eae192d4ade841bdd))
* **tiered-storage:** add access pattern predictor for tier optimization ([cdc5246](https://github.com/josedab/chronicle/commit/cdc5246f1f84596d1103b88ec2f9fe5ea47c33e6))
* **tiered-storage:** add cost dashboard, re-encoding, and HTTP API ([31b3fb1](https://github.com/josedab/chronicle/commit/31b3fb1adea265c9ec626cac9a6c2c39ca2c0d86))
* **tls:** add mTLS authentication and certificate management ([17531b4](https://github.com/josedab/chronicle/commit/17531b4dc37361ffaa88b562b8bd30d87c8a3893))
* **traces:** add distributed trace store with span management ([747a170](https://github.com/josedab/chronicle/commit/747a1702dc41d4412037456e142df944db62fb3d))
* **traces:** add span processors, exemplar linking, and cardinality controls ([3a14482](https://github.com/josedab/chronicle/commit/3a1448250ef734f336ac1f6de6bb010df285e7bb))
* **traces:** add trace-to-metrics derivation pipeline ([07c62f4](https://github.com/josedab/chronicle/commit/07c62f403f17ff5acf82181fb2b21eea723374d0))
* **util:** add escapeSQL helper for SQL injection prevention ([17358db](https://github.com/josedab/chronicle/commit/17358dbfb2161f5095760ecc861837399f3425b4))
* **util:** add quiet cleanup helper functions ([b11623c](https://github.com/josedab/chronicle/commit/b11623c09491c578c6a2a3ef80f2f4073834c675))
* **validator,data_contracts:** add format validation and CheckPoint enforcement ([9f25448](https://github.com/josedab/chronicle/commit/9f25448b46a8dff727330327ec01544aab7093cf))
* **validator:** add MaxTagKeyLen limit to PointValidatorConfig ([c5610d3](https://github.com/josedab/chronicle/commit/c5610d36727a61fcbf3ef21fba905e270d80d836))
* **views:** add incremental materialized view refresh engine ([b1a3c45](https://github.com/josedab/chronicle/commit/b1a3c45836bab61b3bbce2b637893393eb2d06ec))
* **wal:** add CRC32 entry checksums and sync error circuit breaker ([f8ab121](https://github.com/josedab/chronicle/commit/f8ab1212418923de497c53d70462dc7cd668c7e6))
* **wasm:** add built-in WASM bytecode interpreter ([6003d9f](https://github.com/josedab/chronicle/commit/6003d9fd7d8905d21c97ea9cc8f275e3ffdef1cb))
* **wasm:** add configurable map and reduce operations in UDF engine ([8df3780](https://github.com/josedab/chronicle/commit/8df3780b8212b0848abf0e9e1f3734ca40e7ccad))
* **wasm:** add plugin archive format, caching, resource limits, and update API ([ab75025](https://github.com/josedab/chronicle/commit/ab750251d4ee5bf1abf1006ef5ae0c9eac296a0a))
* **wasm:** add plugin marketplace with ed25519 signature verification ([a00bac0](https://github.com/josedab/chronicle/commit/a00bac0520ee56798b386cf9b1c4e31033cefa16))
* **webhook:** implement async delivery loop with retry and dedicated HTTP client ([3fc6db6](https://github.com/josedab/chronicle/commit/3fc6db6c68eee4978dad1abc04d101580cac6b4f))
* **zero-copy:** add predicate pushdown, parallel execution, and adaptive executor ([c6aa18e](https://github.com/josedab/chronicle/commit/c6aa18ef8790e5daf387d075e35c0a18870dac14))


### Bug Fixes

* **adminui:** add request body size limits to admin endpoints ([f537b61](https://github.com/josedab/chronicle/commit/f537b6153dbcac05645da1663d1ad84a025927f3))
* **adminui:** limit maximum SSE client connections ([ed36408](https://github.com/josedab/chronicle/commit/ed364087163f655e8c8ceaf1379e7fbf06eb3bfe))
* **adminui:** sanitize metric names in SQL query construction ([5746e58](https://github.com/josedab/chronicle/commit/5746e58bbd377e340959b9d60b8dd8157ee9531f))
* **alerting:** enforce silence windows and bound evaluation query time range ([a2a5e35](https://github.com/josedab/chronicle/commit/a2a5e356636aaca9020ee50ff2c4ef20acbb23ec))
* **alerting:** track notification goroutines with WaitGroup and log failures ([59c55a6](https://github.com/josedab/chronicle/commit/59c55a6558d5910c61033683d00fbbf689f1364d))
* **alerting:** validate webhook URLs to prevent SSRF ([40dd468](https://github.com/josedab/chronicle/commit/40dd468dc24be66867d8d883b7ee60ca0024eb76))
* **auth:** use MaxAuthBodySize and crypto/rand in token creation ([a312036](https://github.com/josedab/chronicle/commit/a312036112856d41a5f2c5fea5ffd9da744b1bb5))
* **auto-remediation:** validate action parameters and attach webhook payload ([dfcd031](https://github.com/josedab/chronicle/commit/dfcd031d5fdef5286540799ff157072db522ef98))
* **backup:** prevent path traversal in backup deletion ([6715cee](https://github.com/josedab/chronicle/commit/6715cee48277e4ca28d0f6e85ef5b2f7fa7e8795))
* **backup:** propagate file close error in restoreFull ([ad59905](https://github.com/josedab/chronicle/commit/ad59905f2e83f65e8853b199bbfa29c562e51c06))
* **backup:** query per-metric in PITR and implement hot backup snapshot/restore ([f8f8ad0](https://github.com/josedab/chronicle/commit/f8f8ad06517fbffb009772bcab456986b066d650))
* **backup:** thread caller context through backup operations ([224e0bf](https://github.com/josedab/chronicle/commit/224e0bfe97f736dc2485f915389c9b0bb576773a))
* **cache:** add result cache TTL reaper, exemplar write-time TTL, write-path integrations ([7adf789](https://github.com/josedab/chronicle/commit/7adf789b5511205d156105fa6a8dbec1bb9a357d))
* **cardinality:** add safe type assertions in sync.Map operations ([e88741d](https://github.com/josedab/chronicle/commit/e88741d5d917ce1380cd44b744a1a62c9188d709))
* **cdc:** capture loop variables in subscription cleanup goroutine ([3be124d](https://github.com/josedab/chronicle/commit/3be124dfaa397646a158e3e73f834c8fd8a632cd))
* **clickhouse:** add query type whitelist ([efd7673](https://github.com/josedab/chronicle/commit/efd7673013a340650271d7d0b66e18e77bf22a58))
* **clickhouse:** handle HTTP response write errors ([f7cc854](https://github.com/josedab/chronicle/commit/f7cc85495db69f2433215e74f75177b53c33b7c9))
* **clickhouse:** sanitize error messages in HTTP responses ([76298d3](https://github.com/josedab/chronicle/commit/76298d3b0b1900c9f808f442454154628aa87d91))
* **clickhouse:** validate and cap LIMIT clause in query parser ([b8a3e0c](https://github.com/josedab/chronicle/commit/b8a3e0c9100502b510deda4220bc1305efadc56f))
* **cluster:** protect peers map access with read lock ([8c4af03](https://github.com/josedab/chronicle/commit/8c4af03c2dd4281972b080178ab8e627914509f4))
* **collaborative-query:** validate session inputs and limit body size ([cd1c88f](https://github.com/josedab/chronicle/commit/cd1c88f235c609adc1f2ea32e5dca194df33ee36))
* **compaction:** add crash recovery and fsync durability ([fc8e4bf](https://github.com/josedab/chronicle/commit/fc8e4bfe055c8e1335e7af0eb91feff5d3a7db0b))
* **compliance:** use proper JSON encoding in HTTP responses ([272f00b](https://github.com/josedab/chronicle/commit/272f00bec2edd56f27f7cf17938cb212d27f37e7))
* **concurrency:** WebSocket write serialization, recording rules race, write hooks atomics ([fa9f2f9](https://github.com/josedab/chronicle/commit/fa9f2f993d8c184f8e1cc052e736eab88824d0b0))
* **confidential:** use constant-time comparison for attestation verification ([c7c69cf](https://github.com/josedab/chronicle/commit/c7c69cfe024ae687a03de19013cf19dc1ca4b165))
* **config,retention,cdc:** strengthen validation, fix downsampling window bounds, add CDC sequence ([21007af](https://github.com/josedab/chronicle/commit/21007af2e336c0dd28fb2d8f1f43090d5b9da1f5))
* **config:** add cross-field validation, env overrides, and hot-reload validation ([ef45d62](https://github.com/josedab/chronicle/commit/ef45d621f7cb5e1030cc3e4fdbd40208ca3548bd))
* **config:** hot-reload LogLevel and QueryTimeout as safe fields ([e7c2da2](https://github.com/josedab/chronicle/commit/e7c2da2a8c0da5d548ac74248538473315cf8cf9))
* **config:** replace panic with log in MustBuild ([74494ca](https://github.com/josedab/chronicle/commit/74494ca22e1cc4d1c6a08e29f33ec037aa69d36d))
* **continuous_agg,tag_index,db_retention:** float precision, empty filter, retention loop ([15d961e](https://github.com/josedab/chronicle/commit/15d961e088ae12fad1c3a9c93a736f78559e5517))
* **continuous-queries:** add context-aware stream subscription bridging ([25dd390](https://github.com/josedab/chronicle/commit/25dd3904deab2fa2598284f4ed9751ea296a72b2))
* **core:** guard atomic.Value type assertions with ok-check ([8393fab](https://github.com/josedab/chronicle/commit/8393fabdc2cca76e9ae3d365bc8e0caae1e5234b))
* **cors:** reject wildcard origin in query console ([f2548be](https://github.com/josedab/chronicle/commit/f2548bee6c0668279c5906a23dfd7ace3bec487f))
* **cors:** reject wildcard origins and restrict CORS headers ([fb3a805](https://github.com/josedab/chronicle/commit/fb3a805772baaee2e11c8f6eddcd34b22beb8f18))
* **cql:** propagate OFFSET parse error instead of ignoring it ([5f098a0](https://github.com/josedab/chronicle/commit/5f098a000fb205faea4e6a5aa0b912e9e001beec))
* **crypto:** handle rand.Read errors instead of discarding them ([b9adf49](https://github.com/josedab/chronicle/commit/b9adf4972786868c0872c10c15a3d8992777e7b9))
* **crypto:** replace panic with error returns in ID generation ([cc58b2b](https://github.com/josedab/chronicle/commit/cc58b2b012cb5dba05ac169fa33d8f373641f53a))
* **data:** fix series dedup scope, cardinality tracking, and data store accounting ([2a4f38c](https://github.com/josedab/chronicle/commit/2a4f38c59af4b9b41a521b5c239658667eeb61ae))
* **db:** add closed-DB guards and fix graceful shutdown flush ordering ([11d6844](https://github.com/josedab/chronicle/commit/11d684460ef00bbc4a6c9bae90aefe677c064cfd))
* **db:** guard nil schemaRegistry before Validate/ValidateBatch ([44f6d1b](https://github.com/josedab/chronicle/commit/44f6d1b7e9667651fa08cc23f049c8266879cabd))
* **db:** propagate flush and WAL close errors in DB.Close ([e154d3d](https://github.com/josedab/chronicle/commit/e154d3de0fb1db74a533d514ab8ef7f32729db52))
* **db:** report both flush and WAL close errors on shutdown ([46cbffb](https://github.com/josedab/chronicle/commit/46cbffbda5335bd7a4384b605f3bc1e639b28f73))
* **delta-sync:** handle checksum calculation errors ([c0288b2](https://github.com/josedab/chronicle/commit/c0288b2e60fd36ed4f3319c9ba30e878795ea7a9))
* **delta-sync:** log warning when TLS verification is disabled ([75c020a](https://github.com/josedab/chronicle/commit/75c020ae4880271ee1af5a57975583327d61a21a))
* **delta-sync:** require env var to enable InsecureSkipVerify ([1764ff6](https://github.com/josedab/chronicle/commit/1764ff6aac5bcdd340a59b2d32bafba459efe27a))
* **delta-sync:** use context-aware backoff in sendBatch retries ([4f9f7cb](https://github.com/josedab/chronicle/commit/4f9f7cb12bf50eec0d44134c889d0ebc1b8783fc))
* **digitaltwin:** handle errors in Ditto adapter response parsing ([46fba55](https://github.com/josedab/chronicle/commit/46fba55ce52813dbc92e206d034b64909b606d03))
* **digitaltwin:** use context-aware retry delay in batch flush ([0aaf720](https://github.com/josedab/chronicle/commit/0aaf720216a00a6e54d34580eb2e86022ea51e53))
* **docs:** correct prometheus remote write endpoint path ([45e9d6e](https://github.com/josedab/chronicle/commit/45e9d6ef4fefed32ba88ec9e49a65ed8ac503c3a))
* **edgemesh:** track seed peer goroutines with WaitGroup ([0be219d](https://github.com/josedab/chronicle/commit/0be219d5025652cd78531d99d8809783c75c2eb7))
* **errors:** add sentinel errors and multi-unwrap WALSyncError ([d9aab96](https://github.com/josedab/chronicle/commit/d9aab9672baa43f02e74ce271d42fe3f13ea5914))
* **feature_flags:** default unknown features to disabled for safety ([80a34bd](https://github.com/josedab/chronicle/commit/80a34bd9fcd92b1908fcf30717c8848a8b9a4c57))
* **gitops:** prevent symlink path traversal in directory loading ([aa48665](https://github.com/josedab/chronicle/commit/aa486658e0be2a8804e478e00e156306d700f2b0))
* **goroutine:** add WaitGroup and stop channels for leak-free shutdown ([23c9d35](https://github.com/josedab/chronicle/commit/23c9d35735bb3dbff7821af048c1a9c6b19c10b7))
* **goroutine:** add WaitGroup tracking for graceful shutdown ([b942d10](https://github.com/josedab/chronicle/commit/b942d10b6421aef75acd6b1c78470589dd846927))
* **goroutine:** capture variables and tighten security in closures ([2300587](https://github.com/josedab/chronicle/commit/2300587f3b563da4080a9e05ee5c2d0d899a58cd))
* **grafana:** handle JSON decode errors in search and variable handlers ([4c2cb3e](https://github.com/josedab/chronicle/commit/4c2cb3e64b1fb04b5b4eded4f73f5ba5fd82a36b))
* **grafana:** log WebSocket write errors in stream handler ([0ec6fc0](https://github.com/josedab/chronicle/commit/0ec6fc0f0742d5ef6f5055a6c6b452b8c444b7ce))
* **grafana:** prevent goroutine leak in WebSocket stream handler ([14a4444](https://github.com/josedab/chronicle/commit/14a4444ba7043a90fce133a6d451d58ddd05de20))
* **grafana:** validate annotation time range parameters ([d648ab4](https://github.com/josedab/chronicle/commit/d648ab4bacd3fc1d41afac243d69b558bdd00a12))
* **graphql:** add request body limit and subscription cap ([5c20cdd](https://github.com/josedab/chronicle/commit/5c20cddba07ebad88624c154e7b00ae86e181d5c))
* **graphql:** use cryptographic random for subscription IDs ([3a19a1b](https://github.com/josedab/chronicle/commit/3a19a1b83a710770b6117c5099d041d6c26bbf6c))
* **hardwareaccel:** use errors.Join for proper error wrapping ([7065157](https://github.com/josedab/chronicle/commit/7065157ee843d42ad83fd55a76da9507c1d832e7))
* **histogram:** implement correct bucket merge by index ([a761af2](https://github.com/josedab/chronicle/commit/a761af2402640c006c3fadc735e7cbe0335763ee))
* **html-export:** escape user content to prevent XSS ([eab4168](https://github.com/josedab/chronicle/commit/eab41682c1add90b55253bb18d80c470ed7437f8))
* **http:** add CSRF protection middleware ([f8165f0](https://github.com/josedab/chronicle/commit/f8165f0b3e77e656ba1e5d72e7542524d9aadd85))
* **http:** add request body size limit middleware ([0d8f0e7](https://github.com/josedab/chronicle/commit/0d8f0e7813b6640fdc90de52ecfc5caf8e9e360d))
* **http:** add request body size limits to API endpoints ([dbd6a91](https://github.com/josedab/chronicle/commit/dbd6a9146b1923907b5c02286352e929b9f6d762))
* **http:** add server timeouts to edge-sync example and query console ([d57eca4](https://github.com/josedab/chronicle/commit/d57eca4a30fcd0b173397e939b41f190cb718e93))
* **http:** deprecate API key in query parameters ([e9e2b5c](https://github.com/josedab/chronicle/commit/e9e2b5c7a8ea7b2a958c78093b349e3fa2592167))
* **http:** enforce body size limits on API endpoints ([b93296e](https://github.com/josedab/chronicle/commit/b93296ea65d0361eca989072fcbe2e028a71856f))
* **http:** enforce request body size limits on JSON endpoints ([44a9160](https://github.com/josedab/chronicle/commit/44a916074c39e7cafd68f830fc2dff8eb1e9979e))
* **http:** ensure graceful shutdown waits for serve goroutine ([3988d56](https://github.com/josedab/chronicle/commit/3988d56d8650575cd755d408cfba46397d90f3a5))
* **http:** handle SplitHostPort error in getClientIP ([bd16d87](https://github.com/josedab/chronicle/commit/bd16d87d8b91ce73d886d3722c643304941834c7))
* **http:** prevent error message leakage in responses ([e47fcdc](https://github.com/josedab/chronicle/commit/e47fcdc38bc756abd37749225e9ef323fa524c61))
* **http:** propagate request context to DB write and query operations ([d90835e](https://github.com/josedab/chronicle/commit/d90835efa52f1e69523e8ec4be6c514e797920c4))
* **http:** relax CSRF for API content types and add path-aware CSP ([bd932d5](https://github.com/josedab/chronicle/commit/bd932d5d6f3ae259ed6788f1cee6a9c53ecc23d1))
* **http:** remove query parameter API key support ([5a5a8c8](https://github.com/josedab/chronicle/commit/5a5a8c8b93e1eae2faa44a91c6fd87b1ec23d2d3))
* **http:** restrict admin routes to write-capable API keys ([c801ced](https://github.com/josedab/chronicle/commit/c801ced2b094177d50b2edd9743440db1fff00be))
* **http:** restrict alerting rules endpoint to admin-only ([4e9dc97](https://github.com/josedab/chronicle/commit/4e9dc97a433e4b62f3f71267f8fc26b5920dc149))
* **http:** sanitize error messages in HTTP responses ([d9b4bbd](https://github.com/josedab/chronicle/commit/d9b4bbd9b73ec0f4431b6a1e6dae46707bb28057))
* **http:** sanitize error responses to prevent information leakage ([a2fb792](https://github.com/josedab/chronicle/commit/a2fb792e1dd5bc948f215074647a972c893913d7))
* **http:** sanitize internal error messages in responses ([be82f04](https://github.com/josedab/chronicle/commit/be82f04134a48be5623deeffe619f9627cfd92fb))
* **http:** strengthen CSRF protection middleware ([5c1fff6](https://github.com/josedab/chronicle/commit/5c1fff6dac37b2ed66f11d70f4f370718f1d193d))
* **http:** suppress internal error details in responses ([660e8ec](https://github.com/josedab/chronicle/commit/660e8ec8ef406763a868f205109604f2e23ce859))
* **iceberg:** handle errors in namespace and table operations ([f2e4ac3](https://github.com/josedab/chronicle/commit/f2e4ac314cddbcbd6507bc93402e146a899dfc28))
* **iceberg:** harden path traversal checks and use crypto/rand ([0a31497](https://github.com/josedab/chronicle/commit/0a3149797a393b3597142f44c7c985a4eeac722d))
* **iot-gateway:** add HTTP server timeouts and error handling ([3b0ae80](https://github.com/josedab/chronicle/commit/3b0ae803cee01d420158512c9424d00e493f50a0))
* **json:** handle JSON encode/marshal errors in HTTP and gRPC paths ([9669c8d](https://github.com/josedab/chronicle/commit/9669c8d0b600eaccee3c7f4c18066c1d569d8c30))
* **k8s:** escape Prometheus metric label values ([1eef6cb](https://github.com/josedab/chronicle/commit/1eef6cbbb9bb28ff9c0dc4d4601034c534e6180a))
* **lifecycle,write_hooks,rehydration:** graceful shutdown, hook error logging, real cache sizing ([bb554a1](https://github.com/josedab/chronicle/commit/bb554a1f4ad7de45fc0bd6ae55bc52912329af97))
* **lifecycle:** add WaitGroup to ensure goroutines finish before shutdown ([4e888c1](https://github.com/josedab/chronicle/commit/4e888c1d4bc455ca1b3604307abb4f27f4091029))
* **lifecycle:** prevent double-close panic in Stop() across all engines ([2e3cf7b](https://github.com/josedab/chronicle/commit/2e3cf7b68e8dd1c8df06d2dc718613c38152497b))
* **lsp:** prevent goroutine leak in LSP server lifecycle ([281fc60](https://github.com/josedab/chronicle/commit/281fc6039ad375a8079139a4feae17f6f9ed5aca))
* **lsp:** replace goroutine-per-change with debounce timer for diagnostics ([078c48c](https://github.com/josedab/chronicle/commit/078c48c93770b75ef8cb09746619df6f5819247a))
* **makefile:** move dependency checks to deps-check target ([7e079eb](https://github.com/josedab/chronicle/commit/7e079eb07e7caa63156b052b5f9920c7335ea97c))
* **nil-safety:** handle nil tags in CDC, streaming, and schema compat ([c50614d](https://github.com/josedab/chronicle/commit/c50614da6472b03da5714c98676e3f3bd244bad3))
* **nl-dashboard:** sanitize identifiers in query construction ([a593bd4](https://github.com/josedab/chronicle/commit/a593bd4990cdb187d087227312c7e893916549f7))
* **nl-query:** add input validation and sanitization ([5df4da9](https://github.com/josedab/chronicle/commit/5df4da996ddca00d8df4183b01e4c29c8b10c93d))
* **nl-query:** validate topN limits and forecast parameters ([25cede4](https://github.com/josedab/chronicle/commit/25cede4daa1ec0d43d4a575570ea7770ab2c6055))
* **notebooks:** escape HTML output data in notebook export ([604174f](https://github.com/josedab/chronicle/commit/604174f65eb4ea6b6a2be7a42efd6d79d25d4f50))
* **otel:** add WaitGroup for graceful receiver shutdown ([5ba964e](https://github.com/josedab/chronicle/commit/5ba964ee07a60018e3ad08d3fee0f24712d2688c))
* **otel:** use context-aware retry and lifecycle management ([12adfb1](https://github.com/josedab/chronicle/commit/12adfb1ec3141eccd0c5b34067aa9ad298134ead))
* **parquet:** handle JSON marshal error in tag encoding ([633574e](https://github.com/josedab/chronicle/commit/633574e2c5feab00af14520ac24db0b1d55fbc89))
* **parquet:** prevent path traversal in storage operations ([4dd1332](https://github.com/josedab/chronicle/commit/4dd133285dc88b9ab541682973522cbab6517038))
* **parquet:** propagate write errors in writeSimpleParquet ([375ec50](https://github.com/josedab/chronicle/commit/375ec509667fa8dce524d57f352f660d6e4dd9fd))
* **parquet:** strengthen path traversal prevention in export ([fdea095](https://github.com/josedab/chronicle/commit/fdea09515cb6d27d874aba4c0a078106795299e7))
* **pgwire:** avoid deadlock in handleDescribe by releasing lock before I/O ([710e397](https://github.com/josedab/chronicle/commit/710e397c87d4b7908938c83c613bb852654bfb65))
* **pgwire:** log connection close and protocol write errors ([5e0dfba](https://github.com/josedab/chronicle/commit/5e0dfba607d9a701443dc0da12a6da1317739e06))
* **pgwire:** warn on cleartext password authentication ([5d2f22e](https://github.com/josedab/chronicle/commit/5d2f22ec0ba91644f5d3d06b672dc21a21877971))
* **pluginmkt:** prevent path traversal in plugin loading ([ba086a7](https://github.com/josedab/chronicle/commit/ba086a746208f89e02df4e843a996addc38d7a1a))
* **pluginmkt:** validate plugin ID to prevent path traversal ([92e7b22](https://github.com/josedab/chronicle/commit/92e7b222c59dbfc7162496be62e873b524c6404d))
* **policy:** drain audit channel on context cancellation ([7d9e000](https://github.com/josedab/chronicle/commit/7d9e00047d45c386a26c07a65fed9eda91e043d8))
* **query-assistant:** prevent SQL injection in NL query translation ([a5533ef](https://github.com/josedab/chronicle/commit/a5533efdc296a6ed07813a6f4f1465ed6a874d37))
* **query-assistant:** validate SQL aggregate function names ([5dd116c](https://github.com/josedab/chronicle/commit/5dd116cedb9e9d25d21b9f40f06f6e6cd4aa413e))
* **query-builder:** prevent SQL injection in query generation ([79fe963](https://github.com/josedab/chronicle/commit/79fe9634f0285952d4c12863a2d2c754e7f9088d))
* **query-plan:** sanitize metric identifier in query explanation ([d63c8a5](https://github.com/josedab/chronicle/commit/d63c8a5bc6d3bf409e14707912eface7d066401a))
* **query:** pre-compile regex tag filters, stable aggregation sort, context propagation ([0a8b99e](https://github.com/josedab/chronicle/commit/0a8b99e9d5a47699960ea6506240346bfe0f2506))
* **query:** respect context cancellation in distributed queries ([26d90dd](https://github.com/josedab/chronicle/commit/26d90ddd5bc0458d2413362f75afd44a1f8fbca9))
* **query:** sanitize identifiers in NL query and query assistant ([eb01a76](https://github.com/josedab/chronicle/commit/eb01a76c93326e6f601d771829e1b4f33e57a5b1))
* **query:** sanitize metric names in query suggestions ([9e30e65](https://github.com/josedab/chronicle/commit/9e30e65397c37b62e2c2495dc3e59fd4115bf25b))
* **query:** sanitize SQL identifiers in query generation ([9dfc3cf](https://github.com/josedab/chronicle/commit/9dfc3cf8bd706d140cd8d2476635208242741944))
* **raft:** add request body size limits to Raft RPC handlers ([98e2966](https://github.com/josedab/chronicle/commit/98e29666636a9aa72681d020f54af2892d651885))
* **raft:** capture committed log entries in snapshot data ([462a9ac](https://github.com/josedab/chronicle/commit/462a9ac08723ce92ef8ae13c2ed0b215c88681fc))
* **raft:** log command apply failures instead of silently discarding ([e3d17e2](https://github.com/josedab/chronicle/commit/e3d17e2a1723b77f372fdcd65cc35c1297f801c0))
* **registry:** log feature registration errors instead of discarding ([90eda41](https://github.com/josedab/chronicle/commit/90eda4150a10fd762bfc892e77b6ec1fa0fd7c3c))
* **remediation:** validate webhook URLs to prevent SSRF ([736af54](https://github.com/josedab/chronicle/commit/736af5417fe33dc23dc2e50a0170860b0f88c833))
* **replay:** use context-aware delay in stream replay loop ([4a60a8e](https://github.com/josedab/chronicle/commit/4a60a8e8cb228b1889590cb4d5641956389fa07d))
* **replication,rate_controller:** count dropped points and fix token read accuracy ([bfed9af](https://github.com/josedab/chronicle/commit/bfed9afc870ffff6268a34a3df6e11f20667b490))
* **replication:** harden stop safety and error handling ([afd8e62](https://github.com/josedab/chronicle/commit/afd8e621c6257fdfc144d0bda9b58b97ebaf9b00))
* **retry:** clamp jitter to non-negative and enforce single half-open probe ([2381e7f](https://github.com/josedab/chronicle/commit/2381e7f876eca0aa7779408f7511befc05180cfe))
* **retry:** use cancellable backoff in retry loops ([3ad04a8](https://github.com/josedab/chronicle/commit/3ad04a85045f3e25002649dbd1a990c36d491566))
* **s3:** add safe type assertion in S3 backend read ([6988adc](https://github.com/josedab/chronicle/commit/6988adc89dc6bf70f2f5080853f242a6246d3a00))
* **saas:** remove query parameter API key fallback ([190eff9](https://github.com/josedab/chronicle/commit/190eff97fd23450f33c44705740a87e8592ed7ea))
* **saas:** use cryptographic API keys and uniform auth errors ([2c1d736](https://github.com/josedab/chronicle/commit/2c1d736bf9df755aa17d8e5aef4cd5c8baa30e80))
* **schema-evolution:** respect stop signal during migration throttle ([219acb9](https://github.com/josedab/chronicle/commit/219acb9095f1f29ab6125c5801f02b44db56de49))
* **security:** sanitize error messages and enforce body size limits ([91584ac](https://github.com/josedab/chronicle/commit/91584acc6667efaf37c8ba774d03efde27adddcc))
* **security:** validate file paths in export and parquet backends ([3b5ad84](https://github.com/josedab/chronicle/commit/3b5ad84b139eb789f32ef5cbbe8bccaadaa1d40d))
* **sql:** prevent SQL injection in query assistant and D1 backend ([43d3cf1](https://github.com/josedab/chronicle/commit/43d3cf1953e3084024efc440f6417a42e0f10cda))
* **storage:** log best-effort write errors instead of discarding ([b067728](https://github.com/josedab/chronicle/commit/b0677281405ed9d6ccf49e40aa02937d936274f6))
* **streaming,http:** fix concurrent subscription close and standardize error responses ([8d46b99](https://github.com/josedab/chronicle/commit/8d46b9957336db0557ae7dda3cd6312debd8c156))
* **streaming:** handle websocket write errors gracefully ([b4ce9ad](https://github.com/josedab/chronicle/commit/b4ce9ad0043688398a3c10ab0e528e2915de0a7b))
* **tiered-storage:** prevent goroutine leak in migration engine ([567bd82](https://github.com/josedab/chronicle/commit/567bd825d93659654b204f4a39e726e9bedf346a))
* **tiered-storage:** thread context through cost optimizer and migration loop ([12f8984](https://github.com/josedab/chronicle/commit/12f8984cfc7fb3a3d9129b619346eac8b43b2334))
* **tls-auth:** handle json encode errors in auth handlers ([7c7a712](https://github.com/josedab/chronicle/commit/7c7a71207f78f2e1d90ad94de7c48bb82c10f78d))
* **tls:** require expiration time in JWT claims validation ([931d6e7](https://github.com/josedab/chronicle/commit/931d6e7b98a1fa6531ee37ab0be567c8174d71b7))
* **ts-rag:** prevent template injection in LLM prompts ([87b49db](https://github.com/josedab/chronicle/commit/87b49db1f3ba0929064f879ff669ab1c9f3390f0))
* **util:** add sanitizeIdentifier helper to prevent SQL injection ([ab6359e](https://github.com/josedab/chronicle/commit/ab6359e3132587eb26efe4b37da2c1ce8c5a62dd))
* **validation:** reject NaN/Inf point values and guarantee non-nil Result.Points ([4d2b65b](https://github.com/josedab/chronicle/commit/4d2b65bda8dc7421763b3d041a10765520dd2819))
* **validator:** fix Stop() double-close, reject empty tag keys and non-positive timestamps ([74f8afa](https://github.com/josedab/chronicle/commit/74f8afa4f8fc3b9dec137cafc6b82a0efd51d80f))
* **wal:** log errors from flush, sync, and cleanup operations ([b1b7ce6](https://github.com/josedab/chronicle/commit/b1b7ce6be9694557ac24dc564dbb297e95ec8bd6))
* **wal:** recover from partial rotation failure and wrap errors with context ([0f1af12](https://github.com/josedab/chronicle/commit/0f1af123eaac1ec2fec1f13f2ef3ecd2e10dad0e))
* **wasm:** add request body size limits to marketplace and playground ([af41d32](https://github.com/josedab/chronicle/commit/af41d322a55042d73b58fed317bccf4c2aa7440a))
* **wasm:** annotate intentionally unchecked json.Unmarshal calls ([f67bc3a](https://github.com/josedab/chronicle/commit/f67bc3a487a37915fa774fac26c58819bfd1c6a1))
* **wasm:** escape Unicode line separators and script tags in embed output ([bc0eadd](https://github.com/josedab/chronicle/commit/bc0eadddccdf4df9ed01e39859e1c13bbdc6c48d))
* **wasm:** prevent XSS in playground embed script ([d3135bc](https://github.com/josedab/chronicle/commit/d3135bcfe3e6f225e969427158492088262a52c6))
* **wasm:** prevent XSS in playground HTML output ([7267c0a](https://github.com/josedab/chronicle/commit/7267c0a42f74b72e9170ca634aed7889486ca0be))
* **websocket:** enforce CORS origin restrictions ([4f1b5c8](https://github.com/josedab/chronicle/commit/4f1b5c84d7197871f3ab65273ccb8ba23e9c9d2d))
* **websocket:** replace permissive upgrader with origin-validating factory ([40e8cc0](https://github.com/josedab/chronicle/commit/40e8cc029c145550ae4386576d481fc22e52065d))
* **workers:** improve D1 backend error handling and input sanitization ([2b0333c](https://github.com/josedab/chronicle/commit/2b0333cc7314f5284cc778d21da69f0faae3efd1))
* **workers:** use parameterized SQL in D1 backend to prevent injection ([95a0161](https://github.com/josedab/chronicle/commit/95a016171c56a577d1138980fb40aed71ff71d56))


### Performance Improvements

* **optimizer:** use DB index statistics for cost estimation instead of heuristics ([b54c924](https://github.com/josedab/chronicle/commit/b54c9249c7a862bc4c99fdcf7a17a4f8ae2994cc))
* **sqlite:** use prepared statements for aggregate queries ([290fd62](https://github.com/josedab/chronicle/commit/290fd624d552dcf604d9bf42e7828e03333a61a6))
* **zero-copy:** optimize vectorized aggregator with SIMD-style unrolling ([28f28fc](https://github.com/josedab/chronicle/commit/28f28fcfdf5bd77694e2301f618e21e14cc0da2f))

## [0.2.0-beta] - 2026-02-24

### Status
- **Beta release** — API surface is stabilizing but breaking changes may still occur.
- All previously-failing tests (TestHTTPHealth, TestK8sSidecar_HealthEndpoints, TestImportEngine/invalid_lines_skipped, FuzzPointValidation/seed#1) are now fixed.

### Fixed
- Nil map panic in PointValidatorEngine.Validate when ErrorsByType map is uninitialized
- HTTP /health endpoint returning complex HealthCheckStatus instead of simple {"status": "ok"} response
- K8s sidecar missing /health route (only had /api/v1/sidecar/health)
- ImportEngine parseInfluxLine accepting lines without valid field key=value pairs
- Nil-guard checks added to PromQLLabelReplace, PromQLLabelJoin, and NewSeriesKey

### Changed
- Legacy config fields now emit deprecation warnings via log.Println when used
- db_test.go refactored to use table-driven t.Run subtests for better failure isolation

### Added
- `make test-failing` target for fast iteration on known failing tests
- go:generate script for FeatureManager accessor methods (scripts/gen_feature_accessors.go)

## [0.2.0] - 2026-02-22

### Added
- **Write Pipeline Integration**: PointValidator, WriteHooks, and AuditLog wired into core Write() path
- **Query Middleware Pipeline**: Composable middleware chain wired into core Execute() path
- **Health Check System**: /health, /health/ready, /health/live endpoints for K8s probes
- **19 features promoted to Beta**: including TagIndex, WritePipeline, PointValidator, AuditLog, ResultCache, WALSnapshot, IncrementalBackup, and more
- **Feature Flags**: Runtime enable/disable of experimental features with 3 policies
- **Self-Instrumentation**: 12 OTel-compatible self-monitoring metrics
- **API Deprecation Lifecycle**: Structured deprecation with migration guides and removal timelines
- **Data Masking**: Field-level masking rules (redact/hash/truncate) at query time
- **Tenant Isolation**: Per-tenant memory budgets, query quotas, and storage limits
- **Migration Tool**: Import from InfluxDB line protocol and CSV formats
- **Chaos Recovery Testing**: 5 built-in chaos scenarios with automated recovery validation
- **WAL Snapshots**: Snapshot-based WAL compaction for faster recovery
- **Prophet-style Forecasting v2**: Seasonal decomposition with changepoint detection
- **Wire Protocol Server**: TCP-based binary protocol for non-Go clients
- **Embedded Prometheus Scraper**: Built-in /metrics endpoint scraping
- **Query Plan Visualization**: Execution plan trees with DOT/text output
- **Data Rehydration Pipeline**: Auto-fetch cold data from S3 with LRU cache
- **Comprehensive Benchmark Suite**: 8 end-to-end benchmark functions with real measurements
- **Property-Based Testing**: 6 QuickCheck-style tests for core correctness invariants
- **Fuzzing Harness**: 4 fuzz targets for parser, InfluxDB protocol, point validation, WAL
- **Contributor Welcome Bot**: Automated welcome for first-time contributors
- **5 new Architecture Decision Records**: FeatureManager, Write Pipeline, API Tiers, Query Languages, Query Middleware
- **9 new documentation pages**: benchmarks, demo, launch post, compliance, edge deployment, hardware benchmarks, bounty program, community setup, OpenAPI

### Changed
- Duplicate HTTP route registrations fixed (was causing panic on TestHTTPWriteJSON)
- Grafana backend routes namespaced to /api/v1/grafana/*
- ClickHouse routes namespaced to /api/v1/clickhouse/*
- Health check engine routes moved to /api/v1/health/* (admin routes handle /health)
- BENCHMARKS.md updated with real measured numbers
- FEATURE_MATURITY.md updated with 19 promoted beta features
- APIVersion bumped from "0.1.0" to "0.2.0"
- Multi-arch Dockerfile (amd64/arm64/armv7)
- Security audit CI workflow added
- Build tags on 5 stub files (ebpf, zk_query, tinyml, deno_runtime, jupyter_kernel)
- Dead binary removed from examples/core-only/

### Fixed
- Duplicate HTTP route panic: /api/v1/cluster/stats, /api/v1/audit/*, /api/v1/plugins, /health
- QueryMiddleware infinite recursion (now calls ExecuteContext directly)
- HealthCheck.IsLive() returns false before Start() (HTTP handlers now call Start)
- .gitignore missing entries for edge-sync, core-only, iot-gateway binaries

## [Unreleased]

### Added
- Initial open source release
- Single-file storage with append-only partitions
- Gorilla float compression for efficient storage
- Delta timestamp encoding
- Dictionary tag compression
- SQL-like query parser (limited subset)
- Time-based and size-based retention policies
- Downsampling background workers
- WAL-based crash recovery with rotation
- Optional HTTP API with Influx line protocol support
- Optional Prometheus remote write ingestion
- Continuous queries (materialized views)
- Outbound replication to central endpoint
- Comprehensive documentation and examples
- CI/CD pipeline with GitHub Actions

#### Phase 2 Next-Gen Features (v0.5.0)
- **Chronicle Query Language (CQL)**: Purpose-built query language with WINDOW, GAP_FILL, ALIGN, ASOF JOIN
- **Streaming ETL Pipelines**: Declarative fluent-API stream processing with backpressure and checkpointing
- **Adaptive Tiered Storage**: Cost-aware automatic data migration across Hot/Warm/Cold/Archive tiers
- **Distributed Consensus Hardening**: Log compaction, snapshot transfer, joint consensus, leader transfer
- **Hybrid Vector+Temporal Index**: HNSW approximate nearest-neighbor search combined with temporal partitioning
- **Production Observability Suite**: Self-monitoring metrics, health checks, and HTTP status endpoints
- **Incremental Materialized Views**: Delta-apply materialized query results with shadow verification
- **OpenAPI Specification & SDK Generation**: Auto-generated OpenAPI 3.0 spec with Python/TypeScript SDK templates
- **Chaos Engineering Framework**: Fault injection, network/disk simulation, declarative test scenarios
- **Offline-First CRDT Sync**: Vector clocks, bloom filters, G-counters, LWW-registers, OR-sets for edge sync

#### Next-Gen Features (v0.2.0)
- **Pluggable Storage Backends**: FileBackend, MemoryBackend, S3Backend (placeholder), TieredBackend
- **Grafana Data Source Plugin**: TypeScript scaffold for visualization integration
- **PromQL Subset Support**: Prometheus-compatible query language with `/api/v1/query` and `/api/v1/query_range` endpoints
- **WebAssembly Compilation**: Browser/edge runtime support via `wasm/` package
- **Encryption at Rest**: AES-256-GCM encryption with PBKDF2 key derivation
- **Schema Registry**: Metric validation with required tags, allowed values, and value ranges
- **Alerting Engine**: Threshold-based alerts with webhook notifications
- **OpenTelemetry Receiver**: OTLP JSON ingestion via `/v1/metrics` endpoint
- **Multi-Tenancy**: Namespace isolation with tag-based tenant separation
- **Streaming API**: WebSocket real-time subscriptions via `/stream` endpoint

#### Analytics & Advanced Features (v0.3.0)
- **Time-Series Forecasting**: Holt-Winters triple exponential smoothing, anomaly detection via `/api/v1/forecast`
- **Recording Rules Engine**: Pre-compute expensive queries on schedules
- **Native Histograms**: Prometheus-compatible exponential bucketing histograms via `/api/v1/histogram`
- **Exemplar Support**: Link metrics to distributed traces
- **Cardinality Management**: Track and limit series cardinality with alerts
- **Delta/Incremental Backup**: Full and incremental backups with compression and retention
- **GraphQL API**: Flexible querying via `/graphql` with playground at `/graphql/playground`
- **Admin UI Dashboard**: Web-based administration at `/admin` with query explorer
- **Query Federation**: Query across multiple Chronicle instances

#### ML & Export Features (v0.4.0)
- **Data Export**: Export to CSV, JSON lines, and Parquet-like columnar format with compression
- **Vector Embeddings**: Store and search ML embeddings with k-NN (cosine, euclidean, dot product)
- **Query Assistant**: Natural language to SQL/PromQL translation with caching
- **Continuous Profiling**: Store pprof profiles and correlate with metric spikes

### Security
- HTTP body size limits (10MB max)
- Request validation and timeout handling
- No unsafe package usage
- AES-256-GCM encryption at rest (opt-in)

## [0.1.0] - 2026-02-20

### Added
- Initial public release with core embedded time-series database
- Single-file storage with append-only partitions and Gorilla compression
- SQL-like query parser, PromQL subset, and CQL query language
- Pluggable storage backends (file, memory, S3, tiered)
- WAL-based crash recovery
- Context-aware API: WriteContext, WriteBatchContext, ExecuteContext
- HTTP API with Influx line protocol and Prometheus remote write
- OpenTelemetry OTLP receiver
- Grafana data source plugin scaffold
- Encryption at rest (AES-256-GCM)
- Schema registry, alerting engine, multi-tenancy
- Time-series forecasting and anomaly detection
- Comprehensive documentation site and 6 example projects

### Changed
- Migrated all `interface{}` to `any` for modern Go idioms
- FeatureManager now uses lazy initialization (sync.Once) for non-core features
- Write buffer Drain() uses slice swap instead of copy for better performance
- Query path pre-allocates result slices and skips redundant sort
- WAL batches writes instead of flushing per-write
- CORS middleware now requires explicit allowed origins (no more wildcard)

### Deprecated
- materialized_views.go V1 (use MaterializedViewV2Engine)
- stream_dsl.go V1 (use StreamDSLV2Engine)
- adaptive_compression.go V1 (use BanditCompressor V3)
- adaptive_compression_v2.go V2 (use BanditCompressor V3)

### Security
- Fixed CORS wildcard origin in query console and Grafana backend
- Added SLSA provenance attestation for releases
- Added SBOM (SPDX) generation for supply chain transparency
- Strengthened golangci-lint with gosec, errorlint, and additional linters
- Added benchmark regression CI for performance monitoring

---

## Release Notes Format

For each release, document:

### Added
New features and capabilities.

### Changed
Changes in existing functionality.

### Deprecated
Features that will be removed in future versions.

### Removed
Features that have been removed.

### Fixed
Bug fixes.

### Security
Security-related changes and vulnerability fixes.
