# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 1.0.0 (2026-02-22)


### Features

* **adaptive-optimizer:** add ML-powered adaptive query optimizer ([67df2dd](https://github.com/josedab/chronicle/commit/67df2ddceebc4d38aec1e3c94b2f8dbfb65a011a))
* add feature registry plugin system ([c090978](https://github.com/josedab/chronicle/commit/c0909786aa49ecbcce63016f56b50c4b42e066ff))
* add WASM console and time-series RAG integration ([7f3a38b](https://github.com/josedab/chronicle/commit/7f3a38b2f3f056a447d904b85c639662112464a1))
* **admin-ui:** refactor monolithic admin into modular components ([445683e](https://github.com/josedab/chronicle/commit/445683ea6b1cec38cec31dd63e3e627f2492fc91))
* **admin:** add admin UI ([2f58f14](https://github.com/josedab/chronicle/commit/2f58f147bfeb59f8c1f97480f192ac4a2f889dee))
* **admin:** add query templates and annotations ([06ad600](https://github.com/josedab/chronicle/commit/06ad60055aba23db32c5dd600fc15f4a8c1333a7))
* **alerting:** add alert manager and rules ([d4cfc8f](https://github.com/josedab/chronicle/commit/d4cfc8fb141a99d52078ba77d9eadfe6032fef8d))
* **alerts:** add low-code visual alert builder API ([38f3290](https://github.com/josedab/chronicle/commit/38f3290e119c2b0fa717559c70e864d69d581b21))
* **analytics:** add anomaly detection, forecasting, and correlation ([976ecf2](https://github.com/josedab/chronicle/commit/976ecf2e335f28e508685e35dda41b856febe8ed))
* **analytics:** add cardinality tracking ([7f58101](https://github.com/josedab/chronicle/commit/7f581017a3a2b7ef0f0834ebe277f8ea4ac75a9a))
* **analytics:** add continuous queries ([60837ff](https://github.com/josedab/chronicle/commit/60837fffb1989d2f8114098b01dae8e5bc1bb4d8))
* **analytics:** add downsampling rules ([14627a1](https://github.com/josedab/chronicle/commit/14627a154c21159084c3a6d6bc62205416229843))
* **analytics:** add forecasting engine ([c7deea9](https://github.com/josedab/chronicle/commit/c7deea9f66f982bb534d5ff8e873605e607ddfd6))
* **analytics:** add recording rules engine ([7886b13](https://github.com/josedab/chronicle/commit/7886b1314508c06fa7f8db46ee1150d00114dd46))
* **anomaly-explain:** add AI-powered anomaly explainability engine ([6a2271c](https://github.com/josedab/chronicle/commit/6a2271c6e895d7d10fbefb0758ee4d2096120bc1))
* **anomaly:** add AI-powered anomaly detection ([86f4b51](https://github.com/josedab/chronicle/commit/86f4b51e7c499a515346c33d35f0c17a32c38d16))
* **anomaly:** add anomaly classification system ([66cc599](https://github.com/josedab/chronicle/commit/66cc5999a0e061969ca0587531624fe365fab3dc))
* **anomaly:** add single-point detection and webhook alerts ([6f753a3](https://github.com/josedab/chronicle/commit/6f753a30926dd0a7b6d35012e7420e273ebd6ad5))
* **anomaly:** add streaming anomaly detection pipeline ([62ad53e](https://github.com/josedab/chronicle/commit/62ad53e2444e713f1ab8440e021ebf054b35b9c6))
* **api:** add api_aliases.go and API stability annotations ([3c2043f](https://github.com/josedab/chronicle/commit/3c2043f20b47d95263b2dd4429ddfb6525fcbee5))
* **api:** add GraphQL interface ([8dcc9f5](https://github.com/josedab/chronicle/commit/8dcc9f5e697b8c0bf502ba5a1a0c88edfb77bb4b))
* **api:** add HTTP server and endpoints ([075592e](https://github.com/josedab/chronicle/commit/075592ef291e0a95df1ed3d2414adb970204da41))
* **api:** add OpenTelemetry OTLP support ([238852b](https://github.com/josedab/chronicle/commit/238852b7e1140fe6a91de5bf560680166bdabe2e))
* **api:** add query assistant ([3a392ca](https://github.com/josedab/chronicle/commit/3a392ca6a37661c195f808d60f6d2bee0e4bde3f))
* **api:** add streaming subscriptions ([d0cc8c8](https://github.com/josedab/chronicle/commit/d0cc8c826cfe70db32d9fc846a33a6fdbe337420))
* **api:** bump version to 0.2.0 and add deprecation tracking ([a69cccc](https://github.com/josedab/chronicle/commit/a69ccccb19faf7dd703635a2fb0cacdd26ba6ffc))
* **auto-remediation:** add autonomous anomaly remediation engine ([01144d4](https://github.com/josedab/chronicle/commit/01144d47bb889e94913d446dcd0ba2d86de0b0f9))
* **auto-sharding:** add zero-config auto-sharding with consistent hashing ([286b14f](https://github.com/josedab/chronicle/commit/286b14faa32aef2099687fa7e1365a18d6a0b1a8))
* **autoscaling:** add auto-scaling hints engine ([2b32956](https://github.com/josedab/chronicle/commit/2b32956fea8d75431723b27662464120b505887e))
* **backup:** add hot and incremental backup with recovery systems ([bcf6853](https://github.com/josedab/chronicle/commit/bcf68530c4daef6ab6ad44dee26a98bb2cbb9f0d))
* **blockchain-audit:** add immutable blockchain audit trail with Merkle proofs ([fae1b51](https://github.com/josedab/chronicle/commit/fae1b51f7e7a715d74ff397a5e6d91a00352316c))
* **branching:** add Git-like branching for time-series data ([a6e5610](https://github.com/josedab/chronicle/commit/a6e561005559f87269816a0d8110087a326f01d1))
* **buffer:** add batch operations and concurrent write benchmarks ([6cbb4fc](https://github.com/josedab/chronicle/commit/6cbb4fceba4999c142030daa84701ee5db0923f9))
* **capacity-planning:** add predictive capacity forecasting ([631650b](https://github.com/josedab/chronicle/commit/631650ba827eea0423be1dec48a9a80a2918bbc2))
* **cep:** add complex event processing engine ([4724892](https://github.com/josedab/chronicle/commit/47248925a0cd9959f3983d1eaaa3e760ef887611))
* **chaos:** add chaos engineering framework for fault injection ([c306790](https://github.com/josedab/chronicle/commit/c306790122f815e2d0091c27c2879f04ed506a25))
* **chronicle-studio:** add visual time-series programming IDE ([42da2c0](https://github.com/josedab/chronicle/commit/42da2c08ac7fd829d029a3537e50a88da22bdf9a))
* **clickhouse:** add ClickHouse-compatible HTTP query interface ([a6e34b0](https://github.com/josedab/chronicle/commit/a6e34b081e4611a823d49e97d91b45c8303260c5))
* **cloud-saas:** add Chronicle Cloud SaaS platform engine ([bffc042](https://github.com/josedab/chronicle/commit/bffc04288b9e7a83f9bb5eed98fe6dee38a8aa5d))
* **cloud:** add edge-to-cloud sync protocol ([dfa398b](https://github.com/josedab/chronicle/commit/dfa398b548d8a6d2fdc0d0a15f69a5eb688ece46))
* **cloud:** add multi-cloud sync fabric ([079874a](https://github.com/josedab/chronicle/commit/079874ab77c1c347fd546c04376ead261c09336a))
* **cluster:** add distributed clustering support ([3fdd301](https://github.com/josedab/chronicle/commit/3fdd301dd8d067c200f7fb54a86fd0ec14481206))
* **cluster:** add embedded Raft cluster with leader election ([3b26e18](https://github.com/josedab/chronicle/commit/3b26e1874cd028e0b880496afa90fb38ab161c5a))
* **collaborative-query:** add real-time multi-user query editing ([d5b08f7](https://github.com/josedab/chronicle/commit/d5b08f7519f9c6bc0a9197ee5cbd6c41e6e67726))
* **compliance-packs:** add pre-configured compliance framework packs ([bb17af1](https://github.com/josedab/chronicle/commit/bb17af1ee714f787baf251d512c453aa187a3550))
* **compliance:** add compliance automation suite ([1080d2a](https://github.com/josedab/chronicle/commit/1080d2accf347ee5e6132a2f6e8fe0485db36eb4))
* **compression-advisor:** add ML-driven compression codec recommendation engine ([5dd7d63](https://github.com/josedab/chronicle/commit/5dd7d6303a858e60177bf9aaa996a8203e3e8f32))
* **compression:** add adaptive compression engine ([cf0065a](https://github.com/josedab/chronicle/commit/cf0065a1f946fc0a522b970aeeb597f5e9062cce))
* **compression:** add compression plugin system ([cfebccf](https://github.com/josedab/chronicle/commit/cfebccfa4f95613e065f3f571a8ca5703498f4cb))
* **config:** add ConfigBuilder fluent API ([0dbcbde](https://github.com/josedab/chronicle/commit/0dbcbde06a0bcb82f6914a07fb5525731c1c233f))
* **config:** add hot reload and feature flag systems ([a9a3c6e](https://github.com/josedab/chronicle/commit/a9a3c6e03c32137d063a1f023bf9ee07396ee147))
* **config:** enhance validation with cross-field checks ([a2dc5b8](https://github.com/josedab/chronicle/commit/a2dc5b83cb9cd902796f0621dc7da2e7a47300b6))
* **continuous-queries:** add streaming continuous query engine v2 ([2e74a43](https://github.com/josedab/chronicle/commit/2e74a43fa3f316744b5e48577ade550b0c61f6bd))
* **core:** add configuration management ([b7af593](https://github.com/josedab/chronicle/commit/b7af593b459faa4351af1f41d77a7b94641dc758))
* **core:** add error types and handling ([e2f6b0b](https://github.com/josedab/chronicle/commit/e2f6b0bffad7e06616b035f823839b0061f5aa2d))
* **core:** add feature manager ([536eb30](https://github.com/josedab/chronicle/commit/536eb30c9b3cef11894b8877a478ef409516d4e2))
* **core:** add main database implementation ([4deafc9](https://github.com/josedab/chronicle/commit/4deafc905732f9788c1a0437d61990d81695e03b))
* **core:** add point and tag structures ([65c06cc](https://github.com/josedab/chronicle/commit/65c06cca64b7268163dabe72a4f6d5e345981327))
* **core:** add retry logic with circuit breaker ([02e8478](https://github.com/josedab/chronicle/commit/02e84780d792374282b9a49c717d64e9aa2d1cfb))
* **core:** add utility functions ([e256b03](https://github.com/josedab/chronicle/commit/e256b03d55684865b41c32de54ec09338ec02f97))
* **core:** add WriteError, ConfigError types and improved type documentation ([51ecefe](https://github.com/josedab/chronicle/commit/51ecefe82569c580b439bf51e9ad7dfa907bb41c))
* **core:** expand feature manager with lazy engine accessors ([3d21bf4](https://github.com/josedab/chronicle/commit/3d21bf4cbb573c0296ae8f6e5d0e1459dabfc7b6))
* **cql:** add Chronicle Query Language with window functions ([292fb45](https://github.com/josedab/chronicle/commit/292fb45a7097d8404d7a71a23eeb6634c437fc2e))
* **cross-cloud-tiering:** add multi-cloud hybrid storage tiering orchestrator ([756894d](https://github.com/josedab/chronicle/commit/756894dbf3b705c3d88d3c1fc77baddeffa40a55))
* **dashboard:** add embeddable dashboard and admin UI ([2074671](https://github.com/josedab/chronicle/commit/2074671d452989caa8284d80b36f0941c9040777))
* **data-contracts:** add declarative data quality contracts ([5c6ef1e](https://github.com/josedab/chronicle/commit/5c6ef1e8414614b9bb37758dbd8de6319eaefef2))
* **data-mesh:** add distributed data mesh federation engine ([b1baa4a](https://github.com/josedab/chronicle/commit/b1baa4acd3285b009cfe20d2cebc4461d55746d4))
* **declarative-alerting:** add YAML declarative alerting with test framework ([279ec96](https://github.com/josedab/chronicle/commit/279ec96870e15bd3bbcb4dbc243372f02006c62d))
* **delta-sync:** add efficient delta synchronization protocol ([684432e](https://github.com/josedab/chronicle/commit/684432e3beff27f8fa489259d45ca22f07c42c3c))
* **digital-twin:** add digital twin platform synchronization ([33e0d89](https://github.com/josedab/chronicle/commit/33e0d8934d0de5ed157e5f653adea5ace650fc6d))
* **distributed:** add cluster engine, distributed query, and edge fabric ([9c61534](https://github.com/josedab/chronicle/commit/9c61534cea5b503b16b17ef6cc952ecda5e7b768))
* **edge:** add edge mesh networking and edge-cloud sync ([5e73bfa](https://github.com/josedab/chronicle/commit/5e73bfad6b8e3e29f3852c6ac613bda0aa8b0038))
* **encoding:** add column and block codecs ([9a9ba64](https://github.com/josedab/chronicle/commit/9a9ba6462e84c2043cb7f6e5b51f1e1c0cc89220))
* **encoding:** add delta compression codec ([76cd120](https://github.com/josedab/chronicle/commit/76cd120e9c8d81e9f45e07460da463a25cd1f2c8))
* **encoding:** add dictionary compression ([4a426ed](https://github.com/josedab/chronicle/commit/4a426edcc8781c36a1531d210f53ac898e88c361))
* **encoding:** add gorilla float compression ([3b19de5](https://github.com/josedab/chronicle/commit/3b19de5a5c358b1e1d488f9e92ff369222803169))
* **encoding:** add internal encoding and query packages ([3446cff](https://github.com/josedab/chronicle/commit/3446cffe82d6d33e0d5d899b64163edbf7cfedf0))
* **engine:** add Engine interface for lifecycle management ([65632b3](https://github.com/josedab/chronicle/commit/65632b3dda1d7013119bae7f0e1d4b10d696b9f5))
* **enterprise:** add backup and restore ([c51904d](https://github.com/josedab/chronicle/commit/c51904dc7127feb044c426bc06af1bec776d73e7))
* **enterprise:** add encryption at rest ([ec3a63f](https://github.com/josedab/chronicle/commit/ec3a63fd52d31c01d339a0b4598cb02edc003299))
* **enterprise:** add federation support ([94220ba](https://github.com/josedab/chronicle/commit/94220ba0899fa72203788f6878e59132f6ef5a94))
* **enterprise:** add multi-tenancy support ([30328f5](https://github.com/josedab/chronicle/commit/30328f574b7e00a301191657772f917ad1b85196))
* **enterprise:** add replication support ([78f0c65](https://github.com/josedab/chronicle/commit/78f0c652a467a916d4ca5666feceaec7ede05802))
* **enterprise:** add schema registry ([fa2decb](https://github.com/josedab/chronicle/commit/fa2decbb4e1b4b611cdffe286765ce31b8551c16))
* **etl:** add ETL pipeline manager ([6ac2eb9](https://github.com/josedab/chronicle/commit/6ac2eb9036ad8d6b3e27f6e11c2d3b575b7b075e))
* **export:** add data export functionality ([4bff57a](https://github.com/josedab/chronicle/commit/4bff57a463bbad7b1f41eb06f66dea9f330d493d))
* **extend:** add cross-metric alerting, webhooks, WASM UDF, and SDK ([67cb6a9](https://github.com/josedab/chronicle/commit/67cb6a92d60ed8b7c40469b773ecb420f47bbc73))
* **feature-manager:** add feature toggle system for next-gen capabilities ([7d11e5c](https://github.com/josedab/chronicle/commit/7d11e5c2213c434f4609f67ad8b11ae6a1750a36))
* **feature-manager:** add lazy initialization with sync.Once ([f854d22](https://github.com/josedab/chronicle/commit/f854d22094a652a4f9371eaf85ffb976a66ce6e5))
* **features-v2:** add anomaly correlation, cloud relay, query planner, connector hub ([7fcded5](https://github.com/josedab/chronicle/commit/7fcded5cb0abf0e999d8ef8ea95b6a11550a28b8))
* **features-v2:** add notebooks, SaaS control plane, GitOps, federated ML ([c78ec39](https://github.com/josedab/chronicle/commit/c78ec392cda77b315a8ae342c57671aff36564e0))
* **features-v3:** add adaptive compression v2/v3, multi-model graph, fleet manager ([8e40b1e](https://github.com/josedab/chronicle/commit/8e40b1e51d96cd74f069998ab6f822e3cbbfe68c))
* **features-v3:** add query compiler, TS-RAG, plugin SDK, materialized views v2 ([bd1145b](https://github.com/josedab/chronicle/commit/bd1145b0067ca8f7c3df38b82228966e475fb7b8))
* **features:** add autoscale, CDC, benchmark suite, compliance engine, predictive autoscaling ([a6b001f](https://github.com/josedab/chronicle/commit/a6b001f7bebf50dffabd0feda9c496d93cac93b9))
* **features:** add remaining features and wire into FeatureManager ([2a9fd86](https://github.com/josedab/chronicle/commit/2a9fd86b8afdd81dc1ad64fc83ee62eea0c8459e))
* **features:** wire v4 features into feature manager and HTTP routes ([a8f90e8](https://github.com/josedab/chronicle/commit/a8f90e8218fe7ba1ad1183aa7bb57109e32a4a12))
* **features:** wire v5 features into feature manager and HTTP routes ([07ebcbb](https://github.com/josedab/chronicle/commit/07ebcbbc9e6ca2126711ee9214e709d42bdf8f7c))
* **features:** wire v6 features into feature manager and HTTP routes ([95fa648](https://github.com/josedab/chronicle/commit/95fa6489ac020d6e7465449b921c2a7582c92d2e))
* **features:** wire v8 features into feature manager and HTTP routes ([87b60fd](https://github.com/josedab/chronicle/commit/87b60fd31575ad68204d88012f8df4c533ab21d1))
* **federated-learning:** add cross-edge ML model training ([58774c5](https://github.com/josedab/chronicle/commit/58774c5def0e8e2a0ea429208163c92f5964c4ed))
* **federation:** add privacy-preserving federation ([dc65e1e](https://github.com/josedab/chronicle/commit/dc65e1e7477e183950fbeaca884c76a91cdc1a7c))
* **ffi:** add C FFI bindings for cross-language integration ([094deea](https://github.com/josedab/chronicle/commit/094deea9144d86d6af5a1461bc6015039d3f3afa))
* **ffi:** add FFI bindings for Rust, Python, and Node.js ([03f9cf8](https://github.com/josedab/chronicle/commit/03f9cf8197873cfe74f0a262c7abae70c45f8e2b))
* **flight:** add Apache Arrow Flight RPC interface ([e19ed42](https://github.com/josedab/chronicle/commit/e19ed4281f0d2e7314e2ce6f62e413a656684425))
* **foundation-model:** add time-series foundation model inference engine ([01dafa7](https://github.com/josedab/chronicle/commit/01dafa74c02f1237c87595e606e0d037bc93464c))
* **geo:** add geographic/spatial support ([7239789](https://github.com/josedab/chronicle/commit/72397898ff69d9d5eacc442a8a54df1c8596c542))
* **governance:** add audit logging, data lineage, and masking ([b8c1b9a](https://github.com/josedab/chronicle/commit/b8c1b9aefa63c4e7a5563220372799da90d3de76))
* **grafana:** add variable editor and metric discovery ([cf63f93](https://github.com/josedab/chronicle/commit/cf63f93a123c67d6487e664175bed2132dc979c0))
* **grafana:** implement WebSocket streaming and alert management ([885eb8d](https://github.com/josedab/chronicle/commit/885eb8daa26ce178e0bfd74df3a51b5bced55a9a))
* **hardening:** add production hardening suite ([97388db](https://github.com/josedab/chronicle/commit/97388dbde106aa2ec866a0a9e8cd55912f2c5b25))
* **health:** add health check system with k8s probes ([6ce36e8](https://github.com/josedab/chronicle/commit/6ce36e8e7922bd34918b1bfbebc65f9b8aee8c16))
* **http:** expand HTTP routes for new features and protocols ([65f6da0](https://github.com/josedab/chronicle/commit/65f6da013ae13622b7b1588cbb572135d583aaad))
* **hw-accel:** add hardware-accelerated query engine ([230c5e2](https://github.com/josedab/chronicle/commit/230c5e294d3a7f410d2534f93efc17993a49ade4))
* **indexing:** add hybrid vector-temporal indexing with HNSW ([2c56cb1](https://github.com/josedab/chronicle/commit/2c56cb12ed17904d22f2d2354ed956fc913bc329))
* **ingest:** add gRPC, OTLP, and wire protocol ingestion ([ded6f20](https://github.com/josedab/chronicle/commit/ded6f20c1c62a7fc176693a51f559261588ea25c))
* **integrations:** add ClickHouse protocol, Grafana backend, and OTel distribution ([242942b](https://github.com/josedab/chronicle/commit/242942b94e1f5fdcb03e583ddb3f99368bcd491b))
* **integrations:** add Deno runtime, WASM runtime/playground, Grafana plugin ([1bc86e1](https://github.com/josedab/chronicle/commit/1bc86e123811dac9bea4a066f110bc077bbdc9b4))
* **integrations:** add Terraform provider, K8s CRD, SDK generator, stream/pipeline DSL ([7589f20](https://github.com/josedab/chronicle/commit/7589f20e7a517a175c23696d3c1a5d008b1d4906))
* **iot-device-sdk:** add IoT device metrics collection SDK ([d3cb437](https://github.com/josedab/chronicle/commit/d3cb43739ee937537ad2834931dbb0af02d85433))
* **k8s:** add Kubernetes operator integration ([a7934ef](https://github.com/josedab/chronicle/commit/a7934ef61bb13a468d67ad4e65832de786ea1f74))
* **lsp:** add enhanced LSP server for CQL ([f725191](https://github.com/josedab/chronicle/commit/f7251916e0ac015fece41090783d76540b99c1e2))
* **marketplace:** add plugin marketplace engine ([5a52e93](https://github.com/josedab/chronicle/commit/5a52e9367e8e7e9db9c4c7cdfc8c7b1ecaf03055))
* **materialized-views:** add incremental view materialization ([7f01592](https://github.com/josedab/chronicle/commit/7f0159203ce7d852768d701a14bb8146a470ea7e))
* **metrics-catalog:** add metric discovery and catalog system ([75d23b2](https://github.com/josedab/chronicle/commit/75d23b2fcdbb5e4085a48e59f5f073e6ca0f4288))
* **ml:** add ML inference, AutoML, feature store, and supporting modules ([2d1d495](https://github.com/josedab/chronicle/commit/2d1d495f7160fe2e54960b3cb04be0edbc82f3b4))
* **ml:** add TinyML inference engine for edge anomaly detection ([72fb769](https://github.com/josedab/chronicle/commit/72fb769f1de754ee7302610b6261953d812a709f))
* **mobile-sdk:** add cross-platform mobile SDK framework ([5d3359e](https://github.com/josedab/chronicle/commit/5d3359e26a4e7e6fd1fbe609bc580954cc5931fe))
* **multi-model:** add integrated multi-model store ([5c179d1](https://github.com/josedab/chronicle/commit/5c179d120d0e1dd210d060000bc4ac5f0b6dca5a))
* **multi-region:** add multi-region replication engine ([601a9c3](https://github.com/josedab/chronicle/commit/601a9c353a32abf735e2ad5d5eb7bfa62f8c757a))
* **multi-tenant:** add tenant isolation and rate limiting ([0b6f5d2](https://github.com/josedab/chronicle/commit/0b6f5d2089af9c9734547e19c2648c3bdeb49120))
* **nl-dashboard:** add natural language dashboard generation ([dda9868](https://github.com/josedab/chronicle/commit/dda98687f9225e0304c2fb20b114a7cc5c3a9673))
* **observability:** add distributed tracing correlation ([22e1c88](https://github.com/josedab/chronicle/commit/22e1c880ed5fc6ea3b0d88cd34392016e484019f))
* **observability:** add eBPF collector ([3c96c87](https://github.com/josedab/chronicle/commit/3c96c876664d0e6dfbc44b6508f8d2a4dd0cfbd8))
* **observability:** add self-instrumentation and storage statistics ([7505e61](https://github.com/josedab/chronicle/commit/7505e6151251bb4289272de85e1fb09b899637b6))
* **observability:** add self-monitoring metrics and health checks ([8b3cfd5](https://github.com/josedab/chronicle/commit/8b3cfd5fc5fe319e4042bb543c2ee0cc85cf0406))
* **offline-sync:** add CRDT-based offline-first synchronization ([82d6547](https://github.com/josedab/chronicle/commit/82d65476606e449ac271f05892f27b4bc4bf17b7))
* **openapi:** add auto-generated OpenAPI 3.0 spec with SDK templates ([16131d9](https://github.com/josedab/chronicle/commit/16131d94b23cd843d6b35a5bae342ab8e2364d6c))
* **otel:** add exporter factory and receiver config generation ([d0790ed](https://github.com/josedab/chronicle/commit/d0790ed6c21904bf81c7bafd35d09bf98c0ae98f))
* **otel:** add OpenTelemetry Collector exporter/receiver plugin ([c302e40](https://github.com/josedab/chronicle/commit/c302e4086fbcae0aabd6fb2dd2c44ce427d9af9a))
* **otel:** implement OTel distribution with processors and exporters ([f13adbf](https://github.com/josedab/chronicle/commit/f13adbf1a2fb67969fd680f42bbee64d2d3282ea))
* **parquet:** add Parquet native storage format support ([e4cd1a4](https://github.com/josedab/chronicle/commit/e4cd1a410f9edb1f6afedc3192cb82b3a8b5af49))
* **pgwire:** add PostgreSQL wire protocol bridge ([e78de35](https://github.com/josedab/chronicle/commit/e78de352d94689795404a9d9cc9196b89487ca1c))
* **platform:** add K8s sidecar, Workers runtime, and Jupyter kernel ([c6330ca](https://github.com/josedab/chronicle/commit/c6330cac6f96bcf9117cf63584920ff05d468df4))
* **plugin:** add plugin lifecycle manager and k8s reconciler ([457eeed](https://github.com/josedab/chronicle/commit/457eeedd7f245f803914be7a9310cefa591e05a4))
* **plugins:** add plugin marketplace with sandboxed loading ([7a52c29](https://github.com/josedab/chronicle/commit/7a52c29ba536a2d1a8d393e92bdcc4d049bc7f5d))
* **profile:** add profiling integration ([df30e34](https://github.com/josedab/chronicle/commit/df30e34e6ad88c0724c4219937972a1080c93a0b))
* **prom:** add Prometheus drop-in compatibility layer ([0e44ee8](https://github.com/josedab/chronicle/commit/0e44ee8b951796c24c18b91f3082a5969687abaf))
* **promql:** add binary operators and precedence handling ([9972485](https://github.com/josedab/chronicle/commit/9972485d05def14a0d854e49db477e18ac129287))
* **promql:** implement complete PromQL query parser ([06f27ff](https://github.com/josedab/chronicle/commit/06f27fff39796cc53da7f4d523302d126e71563a))
* **query-builder:** add visual query builder SDK ([100e6b6](https://github.com/josedab/chronicle/commit/100e6b6f3d5aa9d80207d515b66fedee99116b4d))
* **query-cache:** add content-addressed query result cache ([dcf5836](https://github.com/josedab/chronicle/commit/dcf5836bdfb2b396598c66952ecd016c4779f3d8))
* **query:** add adaptive query optimizer with cost-based planning ([42ef002](https://github.com/josedab/chronicle/commit/42ef0024b669afef5406f2e406cb16c6b011ab89))
* **query:** add execution profiler and plan visualization ([0ace335](https://github.com/josedab/chronicle/commit/0ace335486ad153ee451a178e56abad1f5c04317))
* **query:** add line protocol parser ([1bd85d3](https://github.com/josedab/chronicle/commit/1bd85d32dff775b7884ccd8c56115b8dad5aaee8))
* **query:** add LSP server for query editing ([5817450](https://github.com/josedab/chronicle/commit/5817450434ad7acf95f35f2ea9130d3c3ab87762))
* **query:** add middleware chain and cost estimation ([59aa39e](https://github.com/josedab/chronicle/commit/59aa39e3aa1ec77f702353c7ab61ca2d868cda92))
* **query:** add natural language query interface ([2ab4a5f](https://github.com/josedab/chronicle/commit/2ab4a5fd42d9132b0686b66dc41cb0bbfaa390ae))
* **query:** add PromQL support ([363815e](https://github.com/josedab/chronicle/commit/363815efb49f935492e5410e223a89ece40980db))
* **query:** add query engine and aggregation ([1ada7c9](https://github.com/josedab/chronicle/commit/1ada7c98eaff6081456ebc859742966fafd93aba))
* **query:** add regex tag filtering support ([70ad676](https://github.com/josedab/chronicle/commit/70ad676a645f13e3f4ca0f0c9067e84d1fc9e685))
* **query:** add streaming SQL engine ([6d5feb0](https://github.com/josedab/chronicle/commit/6d5feb08f29caa975bfadf6ed20b15056363e455))
* **query:** add time-travel query capabilities ([d59f531](https://github.com/josedab/chronicle/commit/d59f53111a04176aee09d444836161d13c0af1cc))
* **query:** add visual query builder ([d4c318d](https://github.com/josedab/chronicle/commit/d4c318d43094e7347962de3fdfb14f5de812b5a9))
* **raft:** add distributed Raft consensus implementation ([f24c0b4](https://github.com/josedab/chronicle/commit/f24c0b4579c779887a6834c0fb66b867c13675ea))
* **raft:** add log compaction, snapshots, and leader transfer ([83f575b](https://github.com/josedab/chronicle/commit/83f575be0bb8b045cba16a02ce6af85a89318abf))
* **regulatory:** add regulatory compliance automation engine ([180ae30](https://github.com/josedab/chronicle/commit/180ae308359409b888da8980895859e28def5705))
* **replication:** add snapshot-based sync ([34719db](https://github.com/josedab/chronicle/commit/34719db46564449936048fc526f375982644097d))
* **retention:** add smart data retention engine ([25bf770](https://github.com/josedab/chronicle/commit/25bf770ae87fc4cc0fd7f3444b529e724ea73109))
* **root-cause-analysis:** add distributed root cause analysis engine ([f94b0b4](https://github.com/josedab/chronicle/commit/f94b0b4ce1975c2ae5d6f411193041d772ea7065))
* **s3:** enhance storage with multipart uploads and cache TTL ([ddfa475](https://github.com/josedab/chronicle/commit/ddfa4754fedf83ea6c567709e6562d8ab9c645d0))
* **schema-designer:** add visual schema designer with code generation ([3804ace](https://github.com/josedab/chronicle/commit/3804ace2f4c5b26a60ee22ef8a29729dadf09588))
* **schema-inference:** add smart schema inference engine ([24ec2b8](https://github.com/josedab/chronicle/commit/24ec2b8eca3f67dbad15b4d1e69cc9ccf74bd7ad))
* **schema:** add schema evolution and metric lifecycle management ([72e651a](https://github.com/josedab/chronicle/commit/72e651a0509fbc40a71f70bc379f56d5bc5d659d))
* **security:** add confidential computing support ([a295afc](https://github.com/josedab/chronicle/commit/a295afc46c609b19648ff46052100adb84dfc623))
* **semantic-search:** add time-series pattern matching and embeddings ([58c6acb](https://github.com/josedab/chronicle/commit/58c6acb4d1141d6f327a07aad4a3e49ba73cdc33))
* **sql-pipelines:** add SQL-first declarative ETL pipeline engine ([1e06275](https://github.com/josedab/chronicle/commit/1e06275164007237c3d4f7f1593a3ccb1a8621eb))
* **storage:** add btree index structure ([67d6905](https://github.com/josedab/chronicle/commit/67d690598f98396ec7e8d7998a90ead3f0a4878a))
* **storage:** add buffer management ([ad92aea](https://github.com/josedab/chronicle/commit/ad92aead06e7b7b9af954b44c0292b223517a56c))
* **storage:** add exemplar support ([c1468a0](https://github.com/josedab/chronicle/commit/c1468a073ea0b248e1302adfb161779e5f5986d9))
* **storage:** add histogram support ([d0e192f](https://github.com/josedab/chronicle/commit/d0e192f746c7d2cf38cda34aae65384c018f86ac))
* **storage:** add index and data store ([d79f2ca](https://github.com/josedab/chronicle/commit/d79f2ca41eabe4d3832c464288417abe55140ec6))
* **storage:** add multi-model storage engine ([cec1e26](https://github.com/josedab/chronicle/commit/cec1e26d734d10b05d03c7788168c99e2a82f63a))
* **storage:** add partition management ([3744808](https://github.com/josedab/chronicle/commit/37448080b6596a40042430bacee70077fba3a6c0))
* **storage:** add SQLite-compatible storage backend ([327a411](https://github.com/josedab/chronicle/commit/327a4117a97d8ba65be6f0cd78726a7a682e689c))
* **storage:** add storage backend abstraction ([53f0dcb](https://github.com/josedab/chronicle/commit/53f0dcb3383741bc352f83c382f0d7b27c2077b8))
* **storage:** add tiered storage lifecycle manager ([b0b022a](https://github.com/josedab/chronicle/commit/b0b022ad47cbd648a2ee6531a7103d4e19444999))
* **storage:** add vector operations ([42ca342](https://github.com/josedab/chronicle/commit/42ca3421fa54fdfe8b184d54f1849d0eb8e15c3f))
* **storage:** add write-ahead log ([29c1077](https://github.com/josedab/chronicle/commit/29c10779273b38c8b09cc0cd7faf9d9cd4da1577))
* **storage:** enhance WAL with snapshots, compaction, and indexing ([6bba2bd](https://github.com/josedab/chronicle/commit/6bba2bd07053dde04b506accd8666fd50068e797))
* **stream-dsl-v2:** add advanced stream processing DSL engine ([43372a4](https://github.com/josedab/chronicle/commit/43372a44e54de31c07f4d657a8b4139792cd08ee))
* **stream-processing:** add event stream processing with windowing and aggregations ([6469a99](https://github.com/josedab/chronicle/commit/6469a99f10f41f238d831d4326b5d6ce4e59a747))
* **streaming-etl:** add declarative ETL pipelines with backpressure ([82da798](https://github.com/josedab/chronicle/commit/82da798f519fe523fc034e9f3b63a3e341ce87be))
* **studio-enhanced:** add enhanced Chronicle Studio IDE ([d4edd39](https://github.com/josedab/chronicle/commit/d4edd39b75f6a5c954cd10a676be9ff18ca3fdef))
* **tiered-storage:** add cost-aware adaptive tier management ([9d09a36](https://github.com/josedab/chronicle/commit/9d09a360a52523da8941d2007ec02a86d6a4718f))
* **time-travel-debug:** add temporal query debugging engine ([974fedd](https://github.com/josedab/chronicle/commit/974fedd6ae90f94c1c905af80232ebe56245c169))
* **tooling:** add testutil helpers, coverage script, and internal test scaffolding ([30fbe08](https://github.com/josedab/chronicle/commit/30fbe08acb9c3367e24bd4673a6ce6ce0732bcb0))
* **ts-branching:** add Git-like time-series branching support ([b5ca7da](https://github.com/josedab/chronicle/commit/b5ca7daff4c5ec7aeb2f48c6881afb410b047735))
* **ts-diff-merge:** add time-series differential and merge algorithms ([b65b0ea](https://github.com/josedab/chronicle/commit/b65b0ea26f995f3f1baaee1f5aace5bd5c4f1bf2))
* **universal-sdk:** add universal SDK generator engine ([89645a9](https://github.com/josedab/chronicle/commit/89645a9c63df8a37c536e9564793f6cfc1f6e09c))
* **wasm:** add build infrastructure and NPM package ([489708b](https://github.com/josedab/chronicle/commit/489708b71a1d9eecfb87082fdf8344c6161dd94d))
* **website:** add interactive playground and update examples ([fb58c87](https://github.com/josedab/chronicle/commit/fb58c874696b5123d516fe5aa5769529835c9b03))
* **website:** add result sparkline visualization to playground ([5fc11c8](https://github.com/josedab/chronicle/commit/5fc11c81265337bd6e1d058c7ef914d760e7dd86))
* **website:** enhance landing page with playground ([5472d65](https://github.com/josedab/chronicle/commit/5472d65abd8a3dc11054a4954ba5b14ea76d5613))
* **write:** wire point validation and pre/post hooks into pipeline ([6e44568](https://github.com/josedab/chronicle/commit/6e445681a1faa83d308fd2609499453e5870bf6f))
* **zk-proofs:** add zero-knowledge query validation ([0dd78c4](https://github.com/josedab/chronicle/commit/0dd78c48df15876063d2be975dc8501ec1e5cd78))


### Bug Fixes

* **cffi:** replace unsafe.Pointer with uintptr_t for cgo handle safety ([029466c](https://github.com/josedab/chronicle/commit/029466c613a04291ce8f1084d45d4de9d69820dc))
* **compression:** stabilize dictionary output order ([8056fc8](https://github.com/josedab/chronicle/commit/8056fc811eabd17906ab11655a81af6700a24feb))
* **http:** restrict X-Forwarded-For trust to loopback addresses ([3249749](https://github.com/josedab/chronicle/commit/3249749242e236ee2432ffe1ea284299dd97e033))


### Performance Improvements

* **bench:** add benchmark runner and regression detection workflow ([c0c677a](https://github.com/josedab/chronicle/commit/c0c677a44aa78174758196839ad11e2ff546a5c4))

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
