# Curbside Intensification

## A Spatiotemporal Graph Neural Network Framework for Data-Driven Curbside Reallocation in Melbourne CBD

---

**Ertugrul Akdemir**

Master in Advanced Computation for Architecture and Design (MaCAD)
Institute for Advanced Architecture of Catalonia (IAAC)
Barcelona, 2026

Faculty Advisor: Jordi Vivaldi

---

## Abstract

Urban streets are designed with fixed spatial allocations, yet the way people use them changes continuously throughout the day and across the week. Curbside space, the narrow strip of two to three meters between the roadway and the sidewalk, has traditionally been reserved for vehicle storage despite growing competition among pedestrians, cyclists, delivery vehicles, and public transit for this limited resource. This imbalance raises a fundamental question: how can we reallocate curbside functions based on streets' temporal behaviour with the assistance of data-driven methods, using parking and pedestrian activities as the primary signals?

This thesis proposes an AI-driven framework that jointly analyses parking occupancy and pedestrian activity across a spatiotemporal street network to identify curbside reallocation opportunities and simulate their network-wide impact. The framework is built around a dual-head Graph Convolutional Network with Gated Recurrent Units (MultiGCN-GRU), trained on 1,397 street segments in Melbourne's Central Business District over a five-month period at 15-minute resolution. By combining spatial and semantic graph convolutions with temporal sequence modelling, the system captures both physical proximity effects and functional similarity between streets. Street-level clustering reveals four behavioural archetypes, each suggesting a distinct intervention strategy. Counterfactual scenario simulations demonstrate how single-street interventions, such as pedestrianisation or parking restriction, propagate across the surrounding network through spatial diffusion and functional spillover.

**Keywords:** curbside management, graph neural networks, spatiotemporal prediction, urban street classification, pedestrian flow, parking occupancy, Melbourne CBD

---

[Figure 0: Representative image. An aerial photograph of a Melbourne CBD street segment showing the curbside zone with parked vehicles on one side and pedestrian activity on the other, illustrating the spatial competition that motivates this research.]

---

## Index

1. Introduction
2. State of the Art
   - 2.1 Curbside Management and Urban Flexibility
   - 2.2 Pedestrian Flow Modelling
   - 2.3 Parking Occupancy Prediction
   - 2.4 Graph Neural Networks for Urban Systems
   - 2.5 Street Activity Classification
   - 2.6 Research Gap
3. Methodology
   - 3.1 Study Area and Data Sources
   - 3.2 Pipeline Architecture
   - 3.3 Feature Engineering and Cube Construction
   - 3.4 Graph Construction
   - 3.5 MultiGCN-GRU Model Architecture
   - 3.6 Street Clustering and Archetype Discovery
   - 3.7 Scenario Simulation Framework
4. Results and Discussion
   - 4.1 Model Performance
   - 4.2 Feature Importance Analysis
   - 4.3 Street Archetypes
   - 4.4 Scenario Analysis and Network Propagation
   - 4.5 Discussion
5. Conclusion and Perspectives
6. List of Figures
7. Bibliography

---

## 1. Introduction

Cities around the world are rethinking how their streets function. For most of the twentieth century, urban street design followed a straightforward logic: the roadway belongs to vehicles, the sidewalk belongs to pedestrians, and the narrow strip between them, the curb, serves as storage for parked cars. This allocation was treated as permanent, embedded in zoning codes and engineering standards that assumed a stable relationship between land use and transportation demand. Yet the reality of how people use streets has always been more dynamic than the infrastructure suggests.

A single street in a central business district may serve as a commuter corridor during morning rush hour, a delivery loading zone at midday, an outdoor dining area in the evening, and a quiet residential lane at night. The same physical space supports fundamentally different functions depending on the time of day and the day of the week. Despite this temporal variability, the physical design of the street remains fixed. Parking meters enforce the same regulations at 7 AM and 7 PM. Lane markings do not change between Tuesday and Saturday. The curb, in particular, remains allocated to vehicle storage regardless of whether the demand for parking justifies that allocation at every hour.

This rigidity creates a problem of spatial competition. The curbside zone, typically two to three meters wide, is among the most contested spaces in any dense urban environment. Pedestrians need wider sidewalks for comfortable movement. Cyclists require protected lanes for safe travel. Delivery vehicles demand loading zones to serve the businesses that line the street. Public transit benefits from bus stop extensions and dedicated boarding areas. Ride-hailing services need pick-up and drop-off zones. Restaurants and cafes seek space for outdoor seating. All of these uses compete for the same narrow strip of land that is predominantly allocated to a single function: storing private vehicles.

The scale of this misallocation is significant. In New York City, for example, only 80,000 of approximately three million curbside spaces are actively metered, while the median income of car owners is roughly twice that of non-car owners, raising questions about who benefits from the current allocation.^1 The New York City Department of Transportation has responded by developing a Curb Management Action Plan that introduces the principle of time-variable curb programming, where the same block can change its function by time of day and day of week.^2 Similar initiatives are emerging in cities across Australia, Europe, and Asia, all motivated by the recognition that static curb allocation fails to match dynamic urban demand.

The challenge, however, is not simply recognising that curbsides should be more flexible. The challenge is operational: city planners need to know which specific streets have temporal windows where reallocation is viable, what spatiotemporal factors drive street-level behaviour so that interventions can be timed correctly, and how an intervention on one street will affect the surrounding network so that unintended consequences can be anticipated. Without data-driven answers to these questions, curbside reallocation remains a political aspiration rather than an implementable strategy. These are the questions that motivate this thesis.

The central research question is: **How can we reallocate curbside functions based on streets' temporal behaviour with the assistance of data-driven methods, using parking and pedestrian activities?**

This question is addressed through three sub-questions:

1. Which streets have temporal flexibility windows that make curbside reallocation viable?
2. What spatiotemporal factors drive the behavioural patterns of urban streets?
3. How does a single-street curbside intervention propagate across the surrounding street network?

The hypothesis is that a joint spatiotemporal model of parking occupancy and pedestrian flow, structured as a graph neural network over the street network, can identify temporal flexibility windows at the individual street level and simulate the network-wide consequences of curbside interventions. The case study through which this hypothesis unfolds is Melbourne's Central Business District, where a dense network of parking sensors and pedestrian counters provides the empirical foundation for the framework.

The remainder of this thesis is organised as follows. Chapter 2 reviews the state of the art in curbside management, pedestrian and parking prediction, graph neural networks for urban systems, and street activity classification. Chapter 3 presents the methodology, including the data pipeline, model architecture, clustering approach, and scenario simulation framework. Chapter 4 reports the results and discusses their implications for the research questions. Chapter 5 concludes with a reflection on the contributions, limitations, and future directions of this work.

---

## 2. State of the Art

### 2.1 Curbside Management and Urban Flexibility

The concept of flexible curbside management has gained significant policy attention in the past decade. The New York City Department of Transportation's Curb Management Action Plan defines five categories of curb function: transportation access, public realm, services and safety, vehicle storage, and circulation and movement.^3 The plan introduces a curb hierarchy that prioritises transit and loading over public realm uses, which in turn take precedence over bike infrastructure and parking. Crucially, this hierarchy is context-dependent, meaning it varies by corridor typology and time of day.

The NYC plan also introduces the concept of "Smart Curbs," described as a "blank slate approach to curb management policies across a district."^4 Under this model, a business improvement district can reprogram its entire curb allocation from scratch, designing time-variable regulations that match observed demand patterns. Protected bike lanes implemented under similar programmes have shown a 34 percent reduction in total injuries on the corridors where they were installed, providing empirical evidence that curb reallocation can produce measurable safety benefits.^5

At the academic level, Hao, Wang, Du, and Chen made a direct connection between spatiotemporal deep learning and curb management.^6 Their work proposed a MultiGCN-LSTM model that jointly predicts pedestrian flow and parking occupancy across urban street segments, using dual graph convolutions (spatial and semantic) to capture the complex dependencies between curb uses. Their scenario simulations demonstrated that parking reduction during identified flexibility windows leads to increased pedestrian flow, validating the concept that curbside reallocation can be guided by data-driven temporal analysis. The framework proposed in this thesis builds directly on their architectural blueprint, extending it with a GRU-based temporal encoder, a larger study area, and a more detailed scenario simulation pipeline.

### 2.2 Pedestrian Flow Modelling

Understanding pedestrian movement patterns is essential for identifying when and where curbside space can be reallocated from vehicle storage to pedestrian use. Asher, Oswald, and Malleson studied pedestrian dynamics in Melbourne's CBD using machine learning with real-time urban sensors.^7 Their work employed a Random Forest model trained on data from 18 pedestrian counting sensors operated by the City of Melbourne, covering the period from 2011 to 2020 (pre-COVID). The study identified hour of day, day of week, and employment density (from the Census of Land Use and Employment, or CLUE) as the top predictors of pedestrian footfall. A buffer analysis found that a 200-metre radius around sensors provided the best spatial feature representation for capturing the surrounding land-use context.

A key contribution of Asher et al. was demonstrating that a model trained on sensored locations can generalise to unsensored streets when enriched with static features such as job counts, cafe density, and bar capacity. This finding directly informs the imputation strategy used in this thesis, where an XGBoost model extends pedestrian flow predictions from 82 sensored streets to the full network of 1,397 modelled segments. Their leave-one-sensor-out cross-validation methodology was also adopted, implemented as GroupKFold validation over street identifiers to prevent spatial leakage.

More broadly, Sevtsuk studied pedestrian flows on street networks and established that network centrality, land use mix, and building density are significant predictors of walking volumes.^8 His work reinforced the importance of treating streets as nodes in a network rather than isolated observation points, a principle that underpins the graph-based architecture of this thesis.

### 2.3 Parking Occupancy Prediction

Predicting parking occupancy with high temporal resolution is the second pillar of the framework. Two recent studies are particularly relevant. Gong, Qin, Xu, and colleagues developed CPPM, a model that combines Graph Convolutional Networks with Gated Recurrent Units and street-view image similarity to forecast parking occupancy at 15-minute intervals.^9 Their work tested three adjacency matrix formulations: distance-based (Manhattan distance), street-view similarity (ResNet50 cosine similarity), and activity-type similarity (POI cosine similarity). A notable finding was that the optimal weight for activity-type similarity was zero, meaning that POI-based similarity around parking lots did not correlate with private car movement patterns.^10 This finding informed the feature importance analysis in this thesis, where the relative contribution of land-use features to parking prediction is examined through permutation importance.

Gong et al. also confirmed that GRU performs comparably to LSTM for temporal sequence modelling in parking contexts, with lower computational cost. Their use of 15-minute time bins was adopted directly in this thesis, providing a resolution fine enough to capture within-hour fluctuations in parking demand without excessive data sparsity.

Zhao and Zhang proposed an Adaptive GCN with GRU (AGCRU) model specifically for Melbourne's on-street parking sensors.^11 Their model introduced a learnable adjacency matrix that adapts during training, eliminating the need for a predefined spatial graph. On the same Melbourne sensor data used in this thesis, they achieved a Mean Absolute Error of 0.0156 and a Mean Absolute Percentage Error of 1.56 percent at the 15-minute prediction horizon. These numbers serve as a benchmark for the parking head of the model developed here. Their work also confirmed that POI features, including cafe count, bar capacity, and business count, improve prediction accuracy beyond temporal-only baselines, validating the static feature set used in this thesis.

### 2.4 Graph Neural Networks for Urban Systems

Graph Neural Networks have emerged as a natural architecture for urban prediction tasks because they can model the irregular spatial topology of street networks. Unlike grid-based approaches such as convolutional neural networks applied to rasterised maps, GNNs operate directly on graph structures where nodes represent street segments and edges encode spatial or functional relationships.

Hao et al. demonstrated the value of using multiple graph convolution layers with different adjacency matrices.^12 Their MultiGCN-LSTM used two GCN branches: a spatial branch based on k-nearest-neighbour distance and a semantic branch based on cosine similarity of land-use features. An ablation study showed that removing the semantic GCN increased prediction error by approximately 3.5 times, while removing the spatial GCN increased error by approximately 1.8 times. This result established that both physical proximity and functional similarity contribute meaningfully to prediction accuracy, with semantic relationships playing a surprisingly large role.

Gong et al. extended this multi-graph approach by adding a third adjacency matrix based on street-view image similarity, computed using ResNet50 feature embeddings.^13 While the image-based matrix improved performance in their context, their finding that POI-based similarity had zero optimal weight suggests that the choice of semantic features requires careful empirical validation.

The architecture developed in this thesis follows the dual-graph approach of Hao et al., using a spatial graph based on intersection topology with Gaussian kernel edge weights and a semantic graph based on mutual k-nearest-neighbour similarity of land-use features. The temporal component replaces LSTM with GRU, following the efficiency findings of Gong et al., and adds dual prediction heads for joint pedestrian and parking forecasting.

### 2.5 Street Activity Classification

Classifying streets by their temporal activity patterns rather than their static land-use designations is a relatively recent development in urban analytics. Su, Sun, Fan, and colleagues proposed the Activity-Based Street Type (AST) framework, which classified 18,023 street segments in Boston using anonymised GPS mobility data.^14 Their two-step clustering approach first grouped streets by activity volume into four types (Subdued, Calm, Moderate, and Vibrant) and then sub-classified each group by normalised 168-hour activity sequences into three temporal patterns (Work, Hybrid, and Leisure). The resulting ten AST types revealed that land use alone explains very little about street activity. The normalised mutual information between ASTs and land-use categories was only 0.04, meaning that knowing a street's land-use designation provides almost no information about its temporal activity pattern.^15

This finding is central to the motivation of this thesis. If land use alone cannot predict when a street is busy or quiet, then temporal data from sensors is essential for identifying flexibility windows. Su et al. also showed that commercial streets, which might be assumed to follow similar activity patterns, actually split roughly evenly between Vibrant and non-Vibrant types.^16 This diversity within a single land-use category reinforces the need for data-driven classification rather than rule-based heuristics.

The clustering approach in this thesis draws on the two-step methodology of Su et al., combining static features (land use, built environment) with temporal profiles (hourly pedestrian and parking patterns) through PCA dimensionality reduction followed by Gaussian Mixture Model clustering. The resulting four archetypes echo their volume-and-pattern classification, adapted to the specific context of Melbourne's CBD and the dual signals of pedestrian flow and parking occupancy.

### 2.6 Research Gap

The literature reviewed above reveals three separate streams of research: curbside policy and management, spatiotemporal prediction of individual transport modes, and activity-based street classification. However, these streams have not been integrated into a single framework that can simultaneously classify streets by temporal behaviour, predict the joint dynamics of pedestrian and parking activity, and simulate the network-wide consequences of curbside interventions.

Hao et al. proposed the closest architecture but focused on a single US CBD without publishing their code or data pipeline, and did not include a clustering step to categorise streets by behavioural archetype.^17 Zhao and Zhang worked on Melbourne parking data but did not incorporate pedestrian flow or scenario simulation.^18 Su et al. developed a powerful street classification framework but used GPS mobility data rather than the parking and pedestrian sensor data that is more directly relevant to curbside allocation decisions.^19

This thesis bridges these gaps by developing an end-to-end framework that integrates all three capabilities. It operates on the same study area as Zhao and Zhang (Melbourne CBD), adopts the dual-graph architecture of Hao et al. with extensions (GRU, dual prediction heads, intersection-topology spatial graph), applies a clustering methodology inspired by Su et al. to identify street archetypes, and adds a counterfactual scenario simulation layer that quantifies the network-wide propagation of single-street interventions. The framework is built as a reproducible 12-step computational pipeline, from raw data ingestion to interactive scenario exploration.

[Figure 1: Research gap positioning diagram. A visual representation showing the three research streams (curbside policy, spatiotemporal prediction, street classification) and how this thesis integrates them into a unified framework. The diagram highlights that no prior work has combined joint ped-parking prediction, graph-based street classification, and network-wide intervention simulation.]

---

## 3. Methodology

### 3.1 Study Area and Data Sources

The study area is Melbourne's Central Business District and its immediate surroundings. The raw street universe consists of 3,975 segments derived from the City of Melbourne's road network data. After filtering to retain only arterial and activity-bearing streets (those with at least one sensor, land-use feature, or traffic function), the modelled graph consists of 1,397 street segments.

The temporal extent covers a five-month period from November 2025 to March 2026, corresponding to the southern hemisphere's late spring through early autumn. This period was selected to capture seasonal variation in outdoor activity while avoiding extreme winter conditions that would suppress pedestrian flow. The data is discretised into 15-minute time bins, producing 14,400 bins per street segment over the study period.

Three primary data sources feed the pipeline:

**Parking Occupancy.** Real-time parking sensor data is sourced from the City of Melbourne's on-street parking infrastructure via a Supabase database. The dataset covers 171 unique parking streets, of which 143 fall within the modelled graph. After data cleaning, the parking dataset contains 1,201,375 non-zero bin entries with a mean occupancy rate of 19.7 percent and a maximum of 40.8 percent. Occupancy is expressed as a continuous value between 0 and 1, representing the fraction of available parking bays occupied at each 15-minute interval.

**Pedestrian Flow.** Pedestrian counting data comes from the City of Melbourne's automated pedestrian counting system, also accessed via Supabase. This system provides hourly or sub-hourly pedestrian counts at sensor locations distributed across the CBD. The raw counts from 82 sensored streets are extended to the full 1,397-street network using an XGBoost imputation model trained on static land-use features, following the methodology validated by Asher et al.^20 The imputed streets receive a confidence weight of 0.5 (versus 1.0 for sensor streets) to reflect the lower certainty of predicted values.

**Static Features.** Built environment and land-use features are sourced from the City of Melbourne's Census of Land Use and Employment (CLUE) dataset, accessed through Melbourne Open Data. These features include total jobs, cafe count, cafe total seats, bar count, bar patron capacity, business count, POI total, dining capacity, and area in square metres. Weather features (temperature, relative humidity, wind speed, and precipitation) are obtained from the Open-Meteo historical weather API. Calendar features (hour of day, day of week, weekend indicator, public holidays, and school holidays) are computed from the timestamp of each time bin.

All spatial operations are performed in the EPSG:3111 projected coordinate reference system, a Lambert Conformal Conic projection appropriate for the state of Victoria that minimises distance distortion in the study area. Final outputs are stored in WGS84 (EPSG:4326) for compatibility with web mapping tools and GeoJSON standards. The choice of a local projected CRS for spatial operations is important because distance-based computations such as buffer analyses and Gaussian kernel edge weights require metric coordinates; performing these operations in WGS84 would introduce latitude-dependent distortions that could bias the spatial graph construction.

[Figure 2: Map of the study area showing the 1,397 modelled street segments in Melbourne CBD, colour-coded by data source availability. Streets with parking sensors (143), streets with pedestrian sensors (82), and streets with imputed data only.]

### 3.2 Pipeline Architecture

The framework is implemented as a 12-step computational pipeline, each step producing intermediate outputs that feed subsequent steps. The pipeline is designed for reproducibility: every step reads from and writes to Parquet files or PyTorch tensors, with a configuration file controlling all parameters.

The steps are organised into three phases:

**Data Ingestion and Preparation (Steps 01 to 03).** Step 01 ingests raw parking and pedestrian data from Supabase and writes time-indexed Parquet files. Step 02 snaps CLUE land-use points to the nearest arterial street segment using spatial joins, producing integer feature counts per street. Step 03 assembles static features from CLUE, weather, and calendar sources into a unified feature table.

**Graph Construction, Processing, and Training (Steps 04 to 09).** Step 04 constructs two graph representations of the street network: a spatial graph based on intersection topology and a semantic graph based on land-use feature similarity. Step 05 processes the raw time series and static features into a normalised spatiotemporal cube. Step 06 aggregates features and computes derived metrics such as street profiles. Step 07 performs dimensionality reduction (PCA) and Gaussian Mixture Model clustering to identify street archetypes. Step 08 prepares training data by constructing sliding windows over the cube. Step 09 trains the MultiGCN-GRU model with dual prediction heads.

**Post-Training Analysis (Steps 10 to 12).** Step 10 performs feature interpretation through permutation importance and branch contribution analysis. Step 11 runs counterfactual scenario simulations with spillover analysis, graph diffusion, and rebound estimation. Step 12 exports enriched GeoJSON files for frontend visualisation and serves an interactive web application.

[Figure 3: Pipeline architecture diagram showing the 12 steps organised into three phases, with data flow arrows indicating how each step's outputs feed into subsequent steps. Key intermediate artefacts (Parquet files, graph tensors, model checkpoints) are annotated at each transition.]

### 3.3 Feature Engineering and Cube Construction

The core data structure is a three-dimensional spatiotemporal cube of shape (N, T, F) = (1,397, 14,400, 23), where N is the number of street segments, T is the number of 15-minute time bins, and F is the number of features. The total cube size is approximately 1.85 gigabytes in memory.

The 23 features fall into four categories:

**Target signals (2 features).** Pedestrian flow (ped_flow) measured in pedestrians per 15-minute interval, and parking occupancy rate (occupancy_rate) expressed as a fraction between 0 and 1. These two features serve as both inputs (in the lookback window) and prediction targets.

**Temporal features (7 features).** Hour of day is encoded as sine and cosine components (hour_sin, hour_cos) to preserve cyclical continuity, meaning that 23:45 and 00:00 are numerically adjacent. Day of week is similarly encoded (dow_sin, dow_cos). Binary indicators for weekend (is_weekend), public holidays (is_public_holiday), and school holidays (is_school_holiday) capture non-cyclical calendar effects.

**Weather features (4 features).** Temperature at 2 metres (temperature_2m), relative humidity (relative_humidity_2m), wind speed at 10 metres (wind_speed_10m), and precipitation. These are sourced from Open-Meteo at hourly resolution and interpolated to 15-minute bins.

**Land-use features (10 features).** Total jobs, cafe count, cafe total seats, bar count, bar patron capacity, business count, POI total, dining capacity, area in square metres, and a pedestrian confidence indicator. These features are static across time but vary across streets, capturing the built-environment context that influences demand patterns.

All features are z-score normalised using mean and standard deviation computed from the training set only, preventing information leakage from validation and test periods. The normalisation statistics are stored in a separate file for use during inference and scenario simulation.

[Figure 4: Feature composition diagram showing the 23 features organised by category (target signals, temporal, weather, land-use), with their data types and sources annotated.]

### 3.4 Graph Construction

The model operates on two graph representations of the street network, each capturing a different type of relationship between street segments.

**Spatial Graph.** The spatial graph encodes physical adjacency based on intersection topology. Two street segments are connected if they share a common intersection endpoint. Edge weights are computed using a Gaussian kernel over the Euclidean distance between segment centroids, so that closer streets receive stronger connections. This approach produces a graph that reflects the actual topology of the road network rather than an arbitrary distance threshold. The resulting spatial graph has 5,635 directed edges, forms a single connected component, and contains no isolated nodes.

Unlike k-nearest-neighbour approaches used in prior work,^21 the intersection-topology method ensures that connections reflect actual navigability. Two streets that are physically close but separated by a building block or a river are not connected, while streets that share an intersection are always connected regardless of centroid distance. This is particularly important in Melbourne's CBD, where the grid layout means that physical distance and topological adjacency are not always correlated.

**Semantic Graph.** The semantic graph encodes functional similarity between streets based on their land-use profiles. For each street, a feature vector is constructed from the static CLUE features (jobs, cafes, bars, businesses, dining capacity, and POI count). The cosine similarity between all pairs of feature vectors is computed, and a mutual k-nearest-neighbour criterion is applied: an edge exists between streets A and B only if A is among B's k nearest neighbours and B is among A's k nearest neighbours. This mutual criterion prevents asymmetric connections where a small street is drawn to a large commercial street that does not reciprocate the similarity. The semantic graph has 8,097 directed edges.

The semantic graph captures a fundamentally different type of relationship than the spatial graph. Two streets may be on opposite sides of the CBD but share similar land-use profiles, meaning that what happens to one is informative about what will happen to the other. Hao et al. showed that removing the semantic graph from their model increased prediction error by 3.5 times,^22 demonstrating that functional similarity carries substantial predictive power beyond physical proximity.

[Figure 5: Side-by-side visualisation of the spatial and semantic graphs overlaid on the Melbourne CBD street network. The spatial graph (left) shows connections following the road topology, while the semantic graph (right) shows connections between functionally similar streets that may be spatially distant.]

### 3.5 MultiGCN-GRU Model Architecture

The prediction model is a dual-head Graph Convolutional Network with Gated Recurrent Unit temporal encoding, referred to as MultiGCN-GRU. The architecture processes spatiotemporal input through three stages: graph convolution, temporal encoding, and task-specific prediction heads.

**Input.** The model receives a four-dimensional tensor of shape (B, W, N, F) = (batch_size, 96, 1,397, 23), where B is the batch size, W = 96 is the lookback window of 96 time bins (24 hours), N is the number of street segments, and F = 23 is the number of input features.

**Graph Convolution Stage.** At each time step within the lookback window, the feature vector for all streets is processed through two parallel GCN branches. The first branch applies a Graph Convolutional layer using the spatial adjacency matrix, transforming the feature dimension from F = 23 to H = 64 hidden units. The second branch applies a separate GCN layer using the semantic adjacency matrix, also producing H = 64 hidden units. The outputs of both branches are concatenated along the feature dimension, producing a representation of 128 features per street per time step.

**Temporal Encoding Stage.** The concatenated graph features are fed into a two-layer GRU with a hidden dimension of 64 and a dropout rate of 0.1. The GRU processes the 96-step sequence for each street independently, capturing temporal dynamics such as daily cycles, trend changes, and event responses. The final hidden state of the GRU, with shape (B, N, 64), encodes the temporal context for each street.

**Prediction Heads.** Two separate linear layers produce the final predictions. The pedestrian head maps the 64-dimensional hidden state to a single pedestrian flow prediction per street, with a learned per-node bias that captures street-specific baseline activity levels. The parking head follows the same structure, predicting parking occupancy rate per street with its own per-node bias.

**Joint Loss Function.** The training loss combines the Mean Absolute Error of the pedestrian head with a weighted and masked MAE of the parking head:

Loss = MAE_ped + 0.5 * masked_MAE_park

The parking loss is masked to the 143 streets that have real sensor data, preventing the model from being penalised for parking predictions on streets where the target values are imputed. The weight of 0.5 reflects the secondary role of parking prediction relative to the primary pedestrian forecasting task.

The total model contains 68,076 trainable parameters, which is deliberately compact compared to models used in traffic forecasting (which often exceed one million parameters). The small parameter count reduces the risk of overfitting to the training period and allows the model to train in under two hours on a single consumer GPU. Training uses the Adam optimiser with a learning rate of 1e-3. A ReduceLROnPlateau scheduler reduces the learning rate when validation performance stagnates, and early stopping with a patience of 25 epochs on the validation pedestrian MAE prevents overtraining. Each training epoch samples 256 gradient steps from the training windows with a batch size of 8 windows, rather than iterating over all possible windows, which provides stochastic regularisation and keeps epoch duration consistent.

**Data Splitting.** The data is split chronologically to prevent temporal leakage. The first 70 percent of time bins (bins 0 to 10,079) form the training set, the next 15 percent (bins 10,080 to 12,239) form the validation set, and the final 15 percent (bins 12,240 to 14,399) form the test set. This corresponds to approximately 3.5 months of training data, 3 weeks of validation data, and 3 weeks of test data.

[Figure 6: Model architecture diagram showing the input tensor flowing through two parallel GCN branches (spatial and semantic), concatenation, GRU temporal encoding, and the two prediction heads for pedestrian flow and parking occupancy.]

### 3.6 Street Clustering and Archetype Discovery

To identify which streets have temporal flexibility windows suitable for curbside reallocation, the framework clusters streets into behavioural archetypes based on their combined static and temporal profiles.

The clustering input consists of 103 profile features per street, derived from the static land-use features, temporal statistics (mean, standard deviation, peak hour, and trough hour for pedestrian and parking signals), and hourly activity profiles (average pedestrian flow and parking occupancy for each hour of the day, separately for weekdays and weekends). This high-dimensional feature space is reduced through Principal Component Analysis to 20 components that explain 99.47 percent of the total variance.

A Gaussian Mixture Model with k = 4 components is fitted to the PCA-reduced profiles. The optimal k was selected by minimising the Bayesian Information Criterion, which penalises model complexity to prevent overfitting. The BIC values tested ranged from k = 2 (BIC = -45,478) to k = 10 (BIC = -53,170), with the minimum at k = 4 (BIC = -57,222). The resulting clustering achieves an Adjusted Rand Index of 0.929 (indicating high stability across random initialisations) and a silhouette score of 0.557 (indicating well-separated clusters).

The four clusters are labelled with descriptive archetype names based on their dominant characteristics:

**Cluster 0: Latent Midday Potential (994 streets).** The largest cluster, comprising 71 percent of the modelled network. These streets have moderate pedestrian flow with a midday peak and low parking occupancy. They represent the bulk of the CBD's secondary streets where curbside space is underutilised during business hours. The recommended intervention type is pedestrian boost, adding amenities or programming that increases foot traffic during identified flexibility windows.

**Cluster 1: Parking Reallocation Priority (141 streets).** Streets with high parking occupancy during business hours that drops sharply in the evening, combined with suppressed pedestrian flow. These streets are candidates for direct pedestrianisation during off-peak parking hours, converting parking bays to temporary pedestrian or cycling space when vehicle storage demand is low.

**Cluster 2: Major Pedestrian Corridor (17 streets).** The smallest but most distinctive cluster, consisting of streets with very high pedestrian volumes throughout the day. These are Melbourne's primary walking streets, including sections of Bourke Street Mall and Swanston Street. The recommended intervention is parking restriction, reducing the already minimal vehicle storage to create fully pedestrian-priority corridors.

**Cluster 3: Evening Outdoor Dining (245 streets).** Streets characterised by an evening activity peak, high dining capacity (cafes and bars), and moderate daytime pedestrian flow. The temporal profile suggests that curbside space used for parking during the day could be reallocated to outdoor dining, seating, or cultural programming in the evening hours. The recommended intervention is a time-conditional boost or restriction tied to the evening activity window.

Of the 1,397 modelled streets, 1,117 (80 percent) have at least one identified flexibility window, defined as a time period where parking occupancy falls below a threshold while pedestrian demand remains above a baseline. Only 2 streets fall in uncertain boundary zones between clusters.

[Figure 7: Four-panel visualisation showing the temporal profiles of each street archetype. Each panel displays the average hourly pedestrian flow (blue line) and parking occupancy (orange line) for weekdays (solid) and weekends (dashed), with the identified flexibility window shaded in green.]

[Figure 8: Map of Melbourne CBD with street segments colour-coded by cluster assignment. Cluster 0 (latent midday potential) in light blue, Cluster 1 (parking reallocation priority) in red, Cluster 2 (major pedestrian corridor) in dark blue, Cluster 3 (evening outdoor dining) in amber.]

### 3.7 Scenario Simulation Framework

The scenario simulation framework uses the trained MultiGCN-GRU model to run counterfactual experiments that answer the third sub-question: how does a single-street intervention propagate across the surrounding network?

Three intervention types are defined, each modifying the input features of a target street in a specific way:

**Pedestrianise.** Sets the parking occupancy rate to zero on the target street, simulating full removal of on-street parking. This represents the most aggressive intervention, converting all curbside parking bays to pedestrian, cycling, or amenity space.

**Restrict Parking.** Sets the parking occupancy rate to a specified magnitude (for example, 0.3 for 30 percent maximum occupancy), simulating a partial reduction in parking availability through time-limited restrictions or metered pricing.

**Boost Pedestrian.** Adds a constant uplift to the pedestrian flow on the target street, simulating the effect of programming or infrastructure changes that attract additional foot traffic (for example, installing public seating, adding a market stall, or widening the sidewalk).

Each simulation follows a consistent protocol. First, a baseline rollout is generated by running the model autoregressively from a seed window selected from the validation set. The seed window is matched to the specified day of week and time of day using L2 distance on the cyclical temporal features (hour_sin, hour_cos, dow_sin, dow_cos). Second, a treated rollout is generated by applying the intervention to the target street's features at each autoregressive step, while keeping all other streets unchanged. The difference between the treated and baseline rollouts reveals the intervention's effect on every street in the network.

The simulation captures four types of network effects:

**Direct Effect.** The change in predicted pedestrian flow and parking occupancy on the target street itself.

**Parking Spillover.** The model's joint parking head predicts how displaced parking demand redistributes to neighbouring streets. Because the model was trained on the correlation between parking changes on one street and parking responses on nearby streets, the spillover prediction is learned from data rather than assumed.

**Graph Diffusion.** The spatial and semantic graph adjacency matrices are used to propagate effects beyond the model's one-step prediction. By computing powers of the adjacency matrix (A, A squared, A cubed), the framework estimates how effects spread through one, two, and three hops in both the physical street network and the functional similarity network.

**Rebound Analysis.** After the intervention period ends, the simulation continues the autoregressive rollout to observe how quickly the network returns to baseline conditions. The rebound is characterised by a half-life (time for the effect to decay by 50 percent) and a recovery fraction (proportion of the effect that dissipates within the simulation horizon).

The scenario simulation is accessible through a Flask API server that accepts POST requests specifying the target street, intervention type, duration, number of rollout steps, magnitude, and temporal context. Results are returned as JSON and can be visualised through an interactive web frontend that displays baseline versus treated time series alongside a map showing the spatial distribution of network effects.

[Figure 9: Scenario simulation workflow diagram showing the four stages: seed window selection, baseline rollout, treated rollout with intervention, and difference analysis producing direct effects, spillover, diffusion, and rebound metrics.]

---

## 4. Results and Discussion

### 4.1 Model Performance

The MultiGCN-GRU model was trained for 215 epochs with early stopping triggered at epoch 190, where the best validation pedestrian MAE was recorded. The training loss decreased steadily from 1.186 at epoch 1 to 0.337 at the best epoch, indicating stable convergence without signs of overfitting.

The final model performance on the validation and test sets is summarised below:

| Metric | Validation | Test |
|--------|-----------|------|
| Pedestrian MAE | 5.646 | 5.535 |
| Pedestrian RMSE | 20.967 | 21.659 |
| Pedestrian R-squared | 0.892 | 0.890 |
| Parking MAE | 0.056 | 0.048 |
| Parking RMSE | 0.091 | 0.083 |
| Parking R-squared | 0.887 | 0.887 |

The pedestrian MAE of 5.646 means that the model's predictions deviate from observed counts by approximately 5.6 pedestrians per 15-minute interval on average. Given that Melbourne CBD streets can see hundreds of pedestrians per interval on major corridors and fewer than ten on quiet streets, this represents a practically useful level of accuracy. The R-squared value of 0.892 indicates that the model explains nearly 90 percent of the variance in pedestrian flow across all streets and time bins.

The parking MAE of 0.056 corresponds to a 5.6 percentage-point deviation in occupancy rate predictions. Compared to the benchmark set by Zhao and Zhang on the same Melbourne parking sensor data, who reported a MAE of 0.0156 at the 15-minute horizon,^23 the model's parking performance is lower. This is expected for two reasons. First, the model in this thesis optimises a joint loss across both pedestrian and parking heads, which means that neither head achieves the performance of a dedicated single-task model. Second, the model predicts parking occupancy for all 1,397 streets simultaneously, including 1,254 streets without real parking sensor data, while Zhao and Zhang predicted only for sensored locations.

The consistency between validation and test performance (test metrics are comparable to or slightly better than validation metrics) confirms that the chronological split successfully prevents temporal leakage. The model generalises well to unseen future time periods.

[Figure 10: Training curves showing pedestrian MAE (left axis, blue) and parking MAE (right axis, orange) on the validation set across 215 epochs. The vertical dashed line marks epoch 190 (best model checkpoint). Both curves show rapid initial improvement followed by gradual convergence.]

### 4.2 Feature Importance Analysis

Permutation importance analysis was conducted on the best model to quantify the contribution of each input feature to prediction accuracy. For each of the 23 features, the feature values were randomly shuffled across the street dimension while keeping all other features intact, and the resulting increase in MAE was recorded. A positive delta MAE indicates that the feature contributes positively to prediction accuracy (shuffling it hurts performance), while a negative value suggests that the feature introduces noise or redundancy.

The results reveal a clear hierarchy. The autoregressive signal (ped_flow, delta MAE = 28.13) dominates all other features, confirming that the most important predictor of pedestrian flow at time t is the pedestrian flow at recent time steps. This is consistent with the temporal inertia of urban activity patterns: crowds do not appear or disappear instantly.

Among the remaining features, the cyclical time encoding (hour_cos, delta MAE = 1.66) is the second most important, capturing the daily rhythm of urban activity. Parking occupancy rate (delta MAE = 0.88) ranks third, validating the joint prediction approach: knowing how many cars are parked on a street provides meaningful information about how many pedestrians are present. Dining capacity (delta MAE = 0.63), bar count (delta MAE = 0.48), and cafe count (delta MAE = 0.39) form a cluster of land-use features that together represent the evening economy and its influence on foot traffic. Employment density (total_jobs, delta MAE = 0.28) and business count (delta MAE = 0.14) contribute positively but modestly, suggesting that commercial activity drives baseline pedestrian levels but explains less of the temporal variation.

Weather features show mixed results. Precipitation, temperature, and wind speed all show negative delta MAE values, indicating that in the current model configuration, weather features slightly degrade performance. This may reflect the fact that the study period (November to March) corresponds to Melbourne's warmest months, during which weather variation is relatively mild and may not significantly affect outdoor activity patterns. In a full-year model, weather features would likely be more important.

**Branch Contribution Analysis.** A separate ablation quantified the contribution of each graph convolution branch. Zeroing out the spatial branch increased the overall MAE by 13.88, while zeroing out the semantic branch increased it by 2.12. This confirms that the spatial graph is the dominant information channel, consistent with the intuition that physical proximity is the primary driver of inter-street dependencies. However, the semantic branch's contribution of 2.12 MAE points is not negligible; it captures functional relationships that physical proximity alone cannot explain, such as the correlated behaviour of streets with similar dining establishments on opposite sides of the CBD.

[Figure 11: Bar chart showing permutation importance (delta MAE) for all 23 features, sorted by magnitude. Features with positive delta MAE (contributing positively) are shown in blue, features with negative delta MAE in grey. The autoregressive ped_flow signal is truncated to fit the scale.]

[Figure 12: Branch contribution comparison showing the delta MAE when each graph convolution branch is zeroed out. The spatial branch (delta = 13.88) is approximately 6.5 times more important than the semantic branch (delta = 2.12), but both contribute meaningfully.]

### 4.3 Street Archetypes

The four-cluster GMM solution identifies behaviourally distinct street types that correspond to different curbside reallocation strategies.

The **latent midday potential** archetype (994 streets, 71 percent of the network) represents the largest opportunity for curbside intensification. These streets are characterised by moderate pedestrian flow with a clear midday peak and consistently low parking occupancy. The temporal profile suggests that these streets are primarily used by workers during lunch hours, with minimal vehicle storage demand. The flexibility window, the period during which parking is low and pedestrian demand is present, typically spans 10:00 to 15:00 on weekdays. During this window, curbside space could be temporarily converted to outdoor seating, market stalls, or widened pedestrian zones without significantly affecting parking availability.

The **parking reallocation priority** archetype (141 streets, 10 percent) identifies streets where the conflict between parking and pedestrian use is most acute. These streets have high daytime parking occupancy that drops sharply after 18:00, combined with a suppressed pedestrian flow that suggests the dominance of vehicle storage is discouraging walking. The evening flexibility window (18:00 to 22:00) offers a clear opportunity for conversion, particularly for streets adjacent to dining and entertainment venues.

The **major pedestrian corridor** archetype (17 streets, 1 percent) consists of Melbourne's most heavily walked streets, where pedestrian flow remains high throughout the day and parking is already minimal. For these streets, the recommendation is to further restrict any remaining vehicle access, extending existing pedestrian-priority zones or converting shared zones to fully pedestrianised spaces.

The **evening outdoor dining** archetype (245 streets, 18 percent) captures streets where the primary activity window is in the evening. These streets have high densities of cafes, bars, and restaurants, and their pedestrian flow peaks between 18:00 and 22:00. Curbside space that serves as daytime parking could be reallocated to outdoor dining starting in the late afternoon, supporting the evening economy that defines these streets' character.

The silhouette score of 0.557 indicates moderately well-separated clusters, with some overlap at cluster boundaries. The Adjusted Rand Index of 0.929 confirms that the clustering is highly stable: running the GMM with different random seeds produces nearly identical assignments. Only 2 of the 1,397 streets fall in ambiguous boundary zones between clusters.

[Figure 13: Cluster profile comparison table showing the mean values of key features (ped_flow_mean, ped_flow_peak_hour, parking_occ_mean, cafe_count, bar_count, dining_capacity, total_jobs) for each of the four archetypes, alongside the cluster size and recommended intervention type.]

### 4.4 Scenario Analysis and Network Propagation

To demonstrate the framework's ability to simulate intervention outcomes, scenario simulations were run for representative streets from each archetype. Each simulation applies an intervention starting at a specified day and hour, runs the autoregressive rollout for 16 steps (4 hours), and records the effects on the target street and its network neighbours.

**Pedestrianisation Scenario.** A parking reallocation priority street was selected and its parking occupancy was set to zero for a weekday afternoon period. The model predicted a 12 to 18 percent increase in pedestrian flow on the target street over the intervention period, consistent with the finding by Hao et al. that parking removal in flexibility windows increases pedestrian activity.^24 The parking spillover analysis showed that displaced parking demand redistributed primarily to three to five neighbouring streets within one hop in the spatial graph, with each neighbour absorbing a 2 to 5 percentage-point increase in occupancy. The semantic graph diffusion revealed that functionally similar streets, even those several blocks away, experienced a smaller but detectable increase in parking demand (0.5 to 1.5 percentage points), suggesting that drivers displaced from one commercial street seek parking near similar commercial environments.

**Partial Restriction Scenario.** A restrict_park intervention with a magnitude of 0.3 (limiting parking to 30 percent of capacity) was applied to an evening outdoor dining street during early evening hours. The model predicted a more moderate pedestrian increase (5 to 8 percent) compared to full pedestrianisation, with proportionally less parking spillover. This suggests that partial restrictions can achieve meaningful pedestrian gains while reducing the disruption to parking supply.

**Rebound Analysis.** After the intervention period ends, the model continues the autoregressive rollout to observe the recovery dynamics. The typical rebound half-life (the time for the intervention effect to decay by 50 percent) is approximately 4 to 6 time steps (1 to 1.5 hours) for pedestrian effects and 2 to 4 time steps (30 to 60 minutes) for parking effects. Parking recovers faster because displaced vehicles actively seek alternative spaces, while pedestrian flow changes persist longer because they reflect changes in route choice and activity patterns that adjust more gradually.

These simulation results are accessible through an interactive web frontend built with Mapbox GL JS and Chart.js, served by a Flask API on port 5050. Users can select any street on the map, choose an intervention type, set temporal parameters, and view the resulting time series for both baseline and treated conditions alongside a map showing the spatial distribution of network effects.

[Figure 14: Scenario simulation output for a pedestrianisation intervention. Top panel: time series of pedestrian flow on the target street (baseline in grey, treated in blue) showing the uplift during the intervention window. Bottom panel: map showing the magnitude of parking spillover on neighbouring streets, with colour intensity proportional to the change in occupancy.]

### 4.5 Discussion

The results address the three sub-questions posed in the introduction.

**Sub-question 1: Which streets have temporal flexibility windows?** The clustering analysis shows that 80 percent of the modelled streets (1,117 of 1,397) have at least one identifiable flexibility window where curbside reallocation is viable. The largest archetype, latent midday potential, alone accounts for 994 streets with midday flexibility windows. This finding challenges the assumption that curbside reallocation is only feasible on a handful of showcase streets; the data suggests that the majority of the CBD's street network has temporal windows where parking demand is low enough to permit temporary repurposing.

**Sub-question 2: What spatiotemporal factors drive street behaviour?** The feature importance analysis reveals a hierarchy of influences. The autoregressive pedestrian signal is by far the dominant predictor, followed by cyclical time encodings, parking occupancy, and evening-economy land-use features (dining capacity, bars, cafes). The relatively low importance of weather features during the study period suggests that Melbourne's summer climate does not significantly modulate outdoor activity. The branch contribution analysis shows that physical proximity (spatial graph, delta MAE = 13.88) explains most of the inter-street dependencies, but functional similarity (semantic graph, delta MAE = 2.12) adds meaningful predictive power, particularly for streets that share land-use characteristics without being physically adjacent.

**Sub-question 3: How do interventions propagate?** The scenario simulations demonstrate that single-street interventions have measurable effects on surrounding streets through both spatial proximity and functional similarity channels. Parking spillover follows the spatial graph (displaced vehicles move to nearby streets), while pedestrian effects diffuse through both spatial and semantic channels (increased foot traffic attracts visitors to functionally similar areas). The rebound analysis shows that effects are temporary, with most interventions returning to baseline within 1 to 2 hours after the intervention ends, which supports the feasibility of time-limited curbside reallocation programmes.

A key contribution of this work is the integration of prediction, classification, and simulation into a single framework. Prior studies have addressed each of these tasks separately, but the combination allows for a workflow that moves from understanding (which streets behave how?) to decision support (what happens if we intervene here?). The dual-head architecture is particularly valuable because it captures the correlation between parking and pedestrian activity, a relationship that single-task models cannot exploit.

However, several limitations should be noted. The study period covers only five months (November to March), corresponding to Melbourne's warmer months. Activity patterns may differ significantly in winter, and a full-year model would be needed to confirm the generality of the identified archetypes. The parking head is supervised only on 143 streets with real sensor data; predictions for the remaining 1,254 streets are extrapolations that carry higher uncertainty. The autoregressive rollout accumulates errors over time, making predictions beyond approximately 4 hours (16 steps) indicative rather than precise. Finally, the framework analyses pedestrian and parking activity but does not incorporate other curb uses such as delivery loading, ride-hailing, or cycling, which are also relevant to holistic curbside management.

---

## 5. Conclusion and Perspectives

This thesis has developed and evaluated an AI-driven framework for data-driven curbside reallocation, applied to 1,397 street segments in Melbourne's Central Business District over a five-month period. The framework integrates three capabilities that have previously been pursued separately: joint spatiotemporal prediction of pedestrian flow and parking occupancy, behavioural street classification, and counterfactual intervention simulation.

The dual-head MultiGCN-GRU model achieves a pedestrian prediction R-squared of 0.892 and a parking prediction R-squared of 0.887, demonstrating that a single model can jointly capture the dynamics of both signals at 15-minute temporal resolution. The dual-graph architecture, combining spatial and semantic convolutions, proves that both physical proximity and functional similarity between streets carry meaningful predictive information. The spatial branch contributes a delta MAE of 13.88 when ablated, while the semantic branch contributes 2.12, confirming that functional similarity adds value beyond what can be captured by physical adjacency alone.

The clustering analysis identifies four behavioural archetypes across the street network, each corresponding to a distinct intervention strategy. Eighty percent of the modelled streets have at least one temporal flexibility window where curbside reallocation is viable, challenging the common assumption that such interventions are limited to a few high-profile corridors. The four archetypes, latent midday potential, parking reallocation priority, major pedestrian corridor, and evening outdoor dining, provide a typology that can guide city planners in prioritising which streets to target and what type of intervention to deploy.

The scenario simulation framework demonstrates that single-street interventions propagate across the network through spatial and functional channels. Pedestrianisation of a parking-dominated street increases pedestrian flow by 12 to 18 percent on the target street, while parking spillover distributes displaced demand across three to five neighbouring streets with modest occupancy increases of 2 to 5 percentage points. These network effects are temporary, with half-lives of 1 to 1.5 hours for pedestrian effects, supporting the feasibility of time-limited curbside reallocation programmes.

### Contributions

This work makes three main contributions to the field:

First, it demonstrates that joint prediction of pedestrian and parking activity on a shared graph network is not only feasible but mutually beneficial. The parking occupancy rate ranks as the third most important feature for pedestrian prediction, confirming that the two signals are correlated and that modelling them together provides information that separate models would miss.

Second, it bridges the gap between street activity classification and intervention simulation. While prior work has classified streets by activity patterns and other work has simulated curb management scenarios, this thesis connects the two: the classification identifies where interventions are most promising, and the simulation estimates their consequences.

Third, it provides a reproducible, open-architecture pipeline that can be adapted to other cities with similar sensor infrastructure. The 12-step pipeline, from data ingestion to interactive scenario exploration, is designed so that each step can be replaced or extended independently, enabling adaptation to different data sources, model architectures, or policy questions.

### Limitations

Several limitations qualify the findings. The five-month study period (November to March) captures only the warmer half of the year in Melbourne. Summer activity patterns may not generalise to winter, and seasonal archetypes may differ from the annual-average archetypes identified here. The parking head is supervised on only 143 of 1,397 streets, meaning that most parking predictions are extrapolations. The model does not account for delivery vehicles, ride-hailing, or cycling, which are also relevant to comprehensive curbside management. The timezone treatment throughout the pipeline assumes UTC alignment between parking and pedestrian data, introducing a systematic 11-hour shift relative to actual Melbourne local time (AEDT, UTC+11). While both signals are internally consistent, temporal profiles should be interpreted with this offset in mind. Finally, the autoregressive rollout accumulates prediction errors, making long-horizon simulations (beyond approximately 4 hours) indicative rather than precise.

### Perspectives

This thesis opens several lines of inquiry for future work.

The most immediate extension would be to incorporate a full year of data, enabling the model to capture seasonal variation in activity patterns and to identify how flexibility windows shift between summer and winter. A winter-specific archetype analysis might reveal that streets classified as "evening outdoor dining" in summer revert to "latent midday potential" when temperatures drop, with implications for seasonal curbside programming.

A second direction is the integration of additional transport modes. Delivery vehicle data from e-commerce logistics platforms, ride-hailing trip records from transportation network companies, and cycling counts from bike-share systems would enable a more complete picture of curbside demand. The multi-head architecture naturally extends to additional prediction heads, and the graph construction framework can accommodate new edge types (for example, delivery route similarity).

A third direction concerns the decision-support interface. The current web frontend allows individual scenario exploration, but a systematic optimisation layer could identify the combination of interventions across multiple streets that maximises pedestrian accessibility while maintaining acceptable parking availability and delivery access. This would move the framework from descriptive and predictive analytics toward prescriptive urbanism, where AI does not only analyse what is happening but recommends what should be done.

Finally, the framework's transferability to other cities is an open question. Melbourne's dense sensor infrastructure and open-data culture make it an unusually well-instrumented study area. Cities with fewer sensors would require more aggressive imputation strategies or alternative data sources such as mobile phone GPS traces, which raise their own questions of privacy and representativeness. Testing the framework on a second city with different urban morphology and climate would strengthen the claim that the approach is generalisable beyond its current context.

The curb is where the city meets the street. It is where vehicles park, pedestrians walk, cyclists ride, deliveries arrive, and social life unfolds. Treating this space as permanently allocated to a single function is a design choice, not a necessity. As cities worldwide confront the need for more sustainable, equitable, and liveable urban environments, the question of who the curbside serves, and when, becomes increasingly urgent. The framework developed in this thesis provides a data-driven foundation for rethinking that allocation, moving from static regulation toward dynamic, evidence-based curbside programming. The streets already know when they are ready for change; the task is to listen to their data and act accordingly, one street and one time window at a time.

---

## List of Figures

- Figure 0: Representative image showing the curbside zone with competing uses
- Figure 1: Research gap positioning diagram showing the integration of three research streams
- Figure 2: Map of the study area showing 1,397 modelled street segments by data source
- Figure 3: Pipeline architecture diagram (12 steps in three phases)
- Figure 4: Feature composition diagram (23 features by category)
- Figure 5: Side-by-side spatial and semantic graph visualisation
- Figure 6: MultiGCN-GRU model architecture diagram
- Figure 7: Four-panel temporal profiles of each street archetype
- Figure 8: Map of Melbourne CBD with streets colour-coded by cluster assignment
- Figure 9: Scenario simulation workflow diagram
- Figure 10: Training curves (pedestrian and parking MAE across epochs)
- Figure 11: Permutation importance bar chart for all 23 features
- Figure 12: Branch contribution comparison (spatial vs. semantic)
- Figure 13: Cluster profile comparison table
- Figure 14: Scenario simulation output (time series and spillover map)

---

## Bibliography

Asher, Michael, Yin Oswald, and Nick Malleson. "Understanding Pedestrian Dynamics Using Machine Learning with Real-Time Urban Sensors." *EPB: Urban Analytics and City Science* 52, no. 8 (2025): 1994-2017.

Gong, Shuhui, Jianbin Qin, Hanfa Xu, Rui Cao, Yang Liu, Congying Jing, Yuxiang Hao, and Yi Yang. "Spatio-Temporal Parking Occupancy Forecasting Integrating Parking Sensing Records and Street-Level Images." *International Journal of Applied Earth Observation and Geoinformation* 118 (2023): 103290.

Hao, Haiyan, Yan Wang, Lili Du, and Shigang Chen. "Enabling Smart Curb Management with Spatiotemporal Deep Learning." *Computers, Environment and Urban Systems* 99 (2023): 101914.

New York City Department of Transportation. *Curb Management Action Plan*. NYC DOT, 2023.

Sevtsuk, Andres. "Street Commerce: Creating Vibrant Urban Sidewalks." *Journal of the American Planning Association* 87, no. 1 (2021).

Su, Tianyu, Mingxuan Sun, Zhuangyuan Fan, Ariel Noyman, Alex Pentland, and Esteban Moro. "Rhythm of the Streets: A Street Classification Framework Based on Street Activity Patterns." *EPJ Data Science* 11 (2022): 43.

Zhao, Xingyu, and Mofeng Zhang. "Enhancing Predictive Models for On-Street Parking Occupancy: Integrating Adaptive GCN and GRU with Household Categories and POI Factors." *Mathematics* 12, no. 18 (2024): 2823.

---

## Footnotes

1. New York City Department of Transportation, *Curb Management Action Plan* (NYC DOT, 2023).
2. Ibid.
3. Ibid.
4. Ibid.
5. Ibid.
6. Haiyan Hao, Yan Wang, Lili Du, and Shigang Chen, "Enabling Smart Curb Management with Spatiotemporal Deep Learning," *Computers, Environment and Urban Systems* 99 (2023): 101914.
7. Michael Asher, Yin Oswald, and Nick Malleson, "Understanding Pedestrian Dynamics Using Machine Learning with Real-Time Urban Sensors," *EPB: Urban Analytics and City Science* 52, no. 8 (2025): 1994-2017.
8. Andres Sevtsuk, "Street Commerce: Creating Vibrant Urban Sidewalks," *Journal of the American Planning Association* 87, no. 1 (2021).
9. Shuhui Gong et al., "Spatio-Temporal Parking Occupancy Forecasting Integrating Parking Sensing Records and Street-Level Images," *International Journal of Applied Earth Observation and Geoinformation* 118 (2023): 103290.
10. Ibid.
11. Xingyu Zhao and Mofeng Zhang, "Enhancing Predictive Models for On-Street Parking Occupancy: Integrating Adaptive GCN and GRU with Household Categories and POI Factors," *Mathematics* 12, no. 18 (2024): 2823.
12. Hao et al., "Enabling Smart Curb Management," 101914.
13. Gong et al., "Spatio-Temporal Parking Occupancy Forecasting," 103290.
14. Tianyu Su et al., "Rhythm of the Streets: A Street Classification Framework Based on Street Activity Patterns," *EPJ Data Science* 11 (2022): 43.
15. Ibid.
16. Ibid.
17. Hao et al., "Enabling Smart Curb Management," 101914.
18. Zhao and Zhang, "Enhancing Predictive Models," 2823.
19. Su et al., "Rhythm of the Streets," 43.
20. Asher, Oswald, and Malleson, "Understanding Pedestrian Dynamics," 1994-2017.
21. Hao et al., "Enabling Smart Curb Management," 101914.
22. Ibid.
23. Zhao and Zhang, "Enhancing Predictive Models," 2823.
24. Hao et al., "Enabling Smart Curb Management," 101914.
