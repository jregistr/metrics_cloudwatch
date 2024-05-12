use log::{error, warn};
use metrics::{CounterFn, GaugeFn, HistogramFn, KeyName, Metadata, SharedString};
use std::ops::Deref;
use std::sync::Arc;
use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    mem,
    pin::Pin,
    task::Poll,
    time::{self, Duration, SystemTime},
};
use tokio::sync::mpsc::error::TrySendError;

use crate::{collector, BoxFuture, Error};
use {
    aws_sdk_cloudwatch::{
        primitives::DateTime,
        types::{Dimension, MetricDatum, StandardUnit, StatisticSet},
        Client as CloudwatchClient,
    },
    futures_util::{
        future,
        stream::{self, Stream},
        FutureExt, StreamExt,
    },
    metrics::{GaugeValue, Key, Recorder, Unit},
    tokio::sync::mpsc,
};

type Count = usize;
type HistogramValue = ordered_float::NotNan<f64>;
type Timestamp = u64;
type HashMap<K, V> = std::collections::HashMap<K, V, ahash::RandomState>;

const MAX_CW_METRICS_PER_CALL: usize = 1000;
const MAX_CLOUDWATCH_DIMENSIONS: usize = 30;
const MAX_HISTOGRAM_VALUES: usize = 150;
const MAX_CW_METRICS_PUT_SIZE: usize = 800_000; // Docs say 1Mb but we set our max lower to be safe since we only have a heuristic
const SEND_TIMEOUT: Duration = Duration::from_secs(4);

/// Configuration for the Cloudwatch Metrics exporter
pub struct Config {
    pub cloudwatch_namespace: String,
    pub default_dimensions: BTreeMap<String, String>,
    pub storage_resolution: Resolution,
    pub send_interval_secs: u64,
    pub client: aws_sdk_cloudwatch::Client,
    pub shutdown_signal: future::Shared<BoxFuture<'static, ()>>,
    pub metric_buffer_size: usize,
    pub force_flush_stream: Option<Pin<Box<dyn Stream<Item = ()> + Send>>>,
}

struct CollectorConfig {
    default_dimensions: BTreeMap<String, String>,
    storage_resolution: Resolution,
}

#[derive(Clone, Copy, Debug)]
pub enum Resolution {
    Second,
    Minute,
}

impl Resolution {
    fn as_secs(self) -> i64 {
        match self {
            Self::Second => 1,
            Self::Minute => 60,
        }
    }
}

#[derive(Debug)]
struct CloudwatchUnit(StandardUnit);

impl Deref for CloudwatchUnit {
    type Target = StandardUnit;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<CloudwatchUnit> for StandardUnit {
    fn from(value: CloudwatchUnit) -> Self {
        value.0
    }
}

impl From<Unit> for CloudwatchUnit {
    fn from(value: Unit) -> Self {
        match value {
            Unit::Count => CloudwatchUnit(StandardUnit::Count),
            Unit::Percent => CloudwatchUnit(StandardUnit::Percent),
            Unit::Seconds => CloudwatchUnit(StandardUnit::Seconds),
            Unit::Milliseconds => CloudwatchUnit(StandardUnit::Milliseconds),
            Unit::Microseconds => CloudwatchUnit(StandardUnit::Microseconds),
            Unit::Nanoseconds => CloudwatchUnit(StandardUnit::None),
            Unit::Tebibytes => CloudwatchUnit(StandardUnit::Terabytes),
            Unit::Gigibytes => CloudwatchUnit(StandardUnit::Gigabytes),
            Unit::Mebibytes => CloudwatchUnit(StandardUnit::Megabytes),
            Unit::Kibibytes => CloudwatchUnit(StandardUnit::Kilobytes),
            Unit::Bytes => CloudwatchUnit(StandardUnit::Bytes),
            Unit::TerabitsPerSecond => CloudwatchUnit(StandardUnit::TerabitsSecond),
            Unit::GigabitsPerSecond => CloudwatchUnit(StandardUnit::GigabitsSecond),
            Unit::MegabitsPerSecond => CloudwatchUnit(StandardUnit::MegabitsSecond),
            Unit::KilobitsPerSecond => CloudwatchUnit(StandardUnit::KilobitsSecond),
            Unit::BitsPerSecond => CloudwatchUnit(StandardUnit::BitsSecond),
            Unit::CountPerSecond => CloudwatchUnit(StandardUnit::CountSecond),
        }
    }
}

/// The Recorder is the main connection point between this exporter and the
/// metrics facade. See (metrics)[https://github.com/metrics-rs/metrics].
pub struct CloudwatchRecorder {
    collector_sender: mpsc::Sender<CollectorMessage>,
}

impl CloudwatchRecorder {
    fn describe(&self, key: KeyName, unit: Option<Unit>) {
        let Some(unit) = unit else {
            return;
        };

        let msg = collector::CollectorMessage::describe(key, unit);
        if let Err(e) = self.collector_sender.try_send(msg) {
            error!("Failed to send message to metric collector: {}", e)
        }
    }
}

impl Recorder for CloudwatchRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, _description: SharedString) {
        self.describe(key, unit)
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, _description: SharedString) {
        self.describe(key, unit)
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, _description: SharedString) {
        self.describe(key, unit)
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> metrics::Counter {
        let collector_sender = self.collector_sender.clone();
        metrics::Counter::from_arc(Arc::new(MetricHandler {
            key: key.clone(),
            collector_sender,
        }))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> metrics::Gauge {
        let collector_sender = self.collector_sender.clone();
        metrics::Gauge::from_arc(Arc::new(MetricHandler {
            key: key.clone(),
            collector_sender,
        }))
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> metrics::Histogram {
        let collector_sender = self.collector_sender.clone();
        metrics::Histogram::from_arc(Arc::new(MetricHandler {
            key: key.clone(),
            collector_sender,
        }))
    }
}

struct MetricHandler {
    key: Key,
    collector_sender: mpsc::Sender<CollectorMessage>,
}

impl MetricHandler {
    fn send(&self, metric_data: MetricData) {
        let message = CollectorMessage::Metric {
            key: self.key.to_owned(),
            data: metric_data,
        };
        if let Err(e) = self.collector_sender.try_send(message) {
            match e {
                TrySendError::Full(_) => warn!("Metric drop due to message buffer size exceeded"),
                TrySendError::Closed(_) => warn!("Metrics channel is closed."),
            }
        }
    }

    fn send_gauge(&self, gauge_value: GaugeValue) {
        let metric = MetricData::Gauge(gauge_value);
        self.send(metric);
    }
}

impl CounterFn for MetricHandler {
    fn increment(&self, value: u64) {
        self.send(MetricData::Counter(CounterValue::Increment(value)));
    }

    fn absolute(&self, value: u64) {
        self.send(MetricData::Counter(CounterValue::Absolute(value)));
    }
}

impl GaugeFn for MetricHandler {
    fn increment(&self, value: f64) {
        self.send_gauge(GaugeValue::Increment(value));
    }

    fn decrement(&self, value: f64) {
        self.send_gauge(GaugeValue::Decrement(value));
    }

    fn set(&self, value: f64) {
        self.send_gauge(GaugeValue::Absolute(value));
    }
}

impl HistogramFn for MetricHandler {
    fn record(&self, value: f64) {
        self.send(MetricData::Histogram(HistogramValue::new(value).unwrap()))
    }
}

#[derive(Debug)]
enum CollectorMessage {
    DescribeMetric {
        key_name: KeyName,
        unit: CloudwatchUnit,
    },
    Metric {
        key: Key,
        data: MetricData,
    },
    SendBatch {
        send_all_before: Timestamp,
        emit_sender: mpsc::Sender<Vec<MetricDatum>>,
    },
}

impl CollectorMessage {
    pub fn describe(key_name: KeyName, metric_unit: Unit) -> Self {
        let unit = metric_unit.into();
        CollectorMessage::DescribeMetric { key_name, unit }
    }
}

#[derive(Debug)]
enum MetricData {
    Counter(CounterValue),
    Gauge(GaugeValue),
    Histogram(HistogramValue),
}

#[derive(Debug)]
enum CounterValue {
    Increment(u64),
    Absolute(u64),
}

#[derive(Clone, Debug, Default)]
struct Aggregate {
    counter: Counter,
    counter_absolute: Option<CounterAbsolute>,
    gauge: Option<Gauge>,
    histogram: HashMap<HistogramValue, Count>,
}

#[derive(Clone, Debug, Default)]
pub struct Counter {
    sample_count: u64,
    sum: u64,
}

#[derive(Clone, Debug, Default)]
pub struct CounterAbsolute {
    current: u64,
}

#[derive(Clone, Debug, Default)]
struct Gauge {
    maximum: f64,
    minimum: f64,
    current: f64,
}

#[derive(Clone, Debug)]
struct MetricConfig {
    unit: StandardUnit,
}

/*#[derive(Debug)]
struct MetricMessage {
    key: Key,
    value: Value,
}*/

#[derive(Debug, Default)]
struct Histogram {
    counts: Vec<f64>,
    values: Vec<f64>,
}

struct HistogramDatum {
    count: f64,
    value: f64,
}

pub(crate) async fn init_future(config: Config) -> Result<(), Error> {
    let (_, task) = new(config);
    task.await;
    Ok(())
}

/// Creates a new Cloudwatch Recorder and task for the Collection process.
pub fn new(mut config: Config) -> (CloudwatchRecorder, impl Future<Output = ()>) {
    let (collect_sender, mut collect_receiver) = mpsc::channel(1024);
    let (emit_sender, emit_receiver) = mpsc::channel(config.metric_buffer_size);

    let force_flush_stream = config
        .force_flush_stream
        .take()
        .unwrap_or_else(|| {
            Box::pin(futures_util::stream::empty::<()>()) as Pin<Box<dyn Stream<Item = ()> + Send>>
        })
        .map({
            let emit_sender = emit_sender.clone();
            move |()| CollectorMessage::SendBatch {
                send_all_before: u64::MAX,
                emit_sender: emit_sender.clone(),
            }
        });

    let message_stream = Box::pin(
        stream::select(
            stream::select(
                stream::poll_fn(move |cx| collect_receiver.poll_recv(cx)),
                mk_send_batch_timer(emit_sender.clone(), &config),
            ),
            force_flush_stream,
        )
        .take_until(config.shutdown_signal.clone().map(|_| true)),
    );

    let emitter = mk_emitter(emit_receiver, config.client, config.cloudwatch_namespace);

    let internal_config = CollectorConfig {
        default_dimensions: config.default_dimensions,
        storage_resolution: config.storage_resolution,
    };

    let mut collector = Collector::new(internal_config);
    let collection_fut = async move {
        collector.accept_messages(message_stream).await;
        // Send a final flush on shutdown
        collector
            .accept(CollectorMessage::SendBatch {
                send_all_before: u64::MAX,
                emit_sender,
            })
            .await;
    };
    (
        CloudwatchRecorder {
            collector_sender: collect_sender,
        },
        async move {
            futures_util::join!(collection_fut, emitter);
        },
    )
}

async fn retry_on_throttled<T, E, F>(
    mut action: impl FnMut() -> F,
) -> Result<Result<T, E>, tokio::time::error::Elapsed>
where
    F: Future<Output = Result<T, E>>,
    E: fmt::Display,
{
    match tokio::time::timeout(SEND_TIMEOUT, action()).await {
        Ok(Ok(t)) => Ok(Ok(t)),
        Ok(Err(err)) if err.to_string().contains("Throttling") => {
            tokio::time::sleep(SEND_TIMEOUT).await;
            tokio::time::timeout(SEND_TIMEOUT, action()).await
        }
        Ok(Err(err)) => Ok(Err(err)),
        Err(err) => Err(err),
    }
}

async fn mk_emitter(
    mut emit_receiver: mpsc::Receiver<Vec<MetricDatum>>,
    cloudwatch_client: CloudwatchClient,
    cloudwatch_namespace: String,
) {
    let cloudwatch_client = &cloudwatch_client;
    let cloudwatch_namespace = &cloudwatch_namespace;
    while let Some(metrics) = emit_receiver.recv().await {
        let chunks: Vec<_> = metrics_chunks(&metrics).collect();
        stream::iter(chunks)
            .for_each(|metric_data| async move {
                let send_fut = retry_on_throttled(|| async {
                    cloudwatch_client
                        .put_metric_data()
                        .set_metric_data(Some(metric_data.to_owned()))
                        .namespace(cloudwatch_namespace)
                        .send()
                        .await
                });

                match send_fut.await {
                    Ok(Ok(_output)) => {
                        log::debug!("Successfully sent a metrics batch to CloudWatch.")
                    }
                    Ok(Err(e)) => log::warn!(
                        "Failed to send metrics: {:?}: {}",
                        metric_data
                            .iter()
                            .map(|m| &m.metric_name)
                            .collect::<Vec<_>>(),
                        e,
                    ),
                    Err(tokio::time::error::Elapsed { .. }) => {
                        log::warn!("Failed to send metrics: send timeout")
                    }
                }
            })
            .await;
    }
}

fn fit_metrics<'a>(metrics: impl IntoIterator<Item = &'a MetricDatum>) -> usize {
    let mut split = 0;

    let mut current_len = 0;
    // PutMetricData uses this really high overhead format so just take a high estimate of that.
    //
    // Assumes each value sent is ~60 bytes
    // ```
    // MetricData.member.2.Dimensions.member.2.Value=m1.small
    // ```
    for (i, metric) in metrics
        .into_iter()
        .take(MAX_CW_METRICS_PER_CALL)
        .enumerate()
    {
        current_len += metric_size(metric);
        if current_len > MAX_CW_METRICS_PUT_SIZE {
            break;
        }
        split = i + 1;
    }
    split
}

fn metrics_chunks(mut metrics: &[MetricDatum]) -> impl Iterator<Item = &[MetricDatum]> + '_ {
    std::iter::from_fn(move || {
        let split = fit_metrics(metrics);
        let (chunk, rest) = metrics.split_at(split);
        metrics = rest;
        if chunk.is_empty() {
            None
        } else {
            Some(chunk)
        }
    })
}

fn metric_size(metric: &MetricDatum) -> usize {
    60 * (
        // The 6 non Vec fields
        6 + &metric.values().len() + &metric.counts().len() + &metric.dimensions().len()
    )
}

fn jitter_interval_at(
    start: tokio::time::Instant,
    interval: Duration,
) -> impl Stream<Item = tokio::time::Instant> {
    use rand::{rngs::SmallRng, Rng, SeedableRng};

    let rng = SmallRng::from_rng(rand::thread_rng()).unwrap();
    let variance = 0.1;
    let interval_secs = interval.as_secs_f64();
    let min = Duration::from_secs_f64(interval_secs * (1.0 - variance));
    let max = Duration::from_secs_f64(interval_secs * (1.0 + variance));

    let delay = Box::pin(tokio::time::sleep_until(start));
    stream::unfold((rng, delay), move |(mut rng, mut delay)| async move {
        (&mut delay).await;
        let now = delay.deadline();
        delay.as_mut().reset(now + rng.gen_range(min..max));
        Some((now, (rng, delay)))
    })
}

fn mk_send_batch_timer(
    emit_sender: mpsc::Sender<Vec<MetricDatum>>,
    config: &Config,
) -> impl Stream<Item = CollectorMessage> {
    let interval = Duration::from_secs(config.send_interval_secs);
    let storage_resolution = config.storage_resolution;
    jitter_interval_at(tokio::time::Instant::now(), interval).map(move |_instant| {
        let send_all_before = time_key(current_timestamp(), storage_resolution) - 1;
        CollectorMessage::SendBatch {
            send_all_before,
            emit_sender: emit_sender.clone(),
        }
    })
}

fn current_timestamp() -> Timestamp {
    time::UNIX_EPOCH.elapsed().unwrap().as_secs()
}

fn datetime(now: SystemTime) -> DateTime {
    use aws_smithy_types_convert::date_time::DateTimeExt;
    let dt = chrono::DateTime::<chrono::offset::Utc>::from(now);
    DateTime::from_chrono_utc(dt)
}

fn time_key(timestamp: Timestamp, resolution: Resolution) -> Timestamp {
    match resolution {
        Resolution::Second => timestamp,
        Resolution::Minute => timestamp - (timestamp % 60),
    }
}

fn get_timeslot<'a>(
    metrics_data: &'a mut BTreeMap<Timestamp, HashMap<Key, Aggregate>>,
    config: &'a CollectorConfig,
    timestamp: Timestamp,
) -> &'a mut HashMap<Key, Aggregate> {
    metrics_data
        .entry(time_key(timestamp, config.storage_resolution))
        .or_default()
}

fn accept_datum(slot: &mut HashMap<Key, Aggregate>, key: Key, data: MetricData) {
    match data {
        MetricData::Counter(value) => match value {
            CounterValue::Increment(value) => {
                let aggregate = slot.entry(key).or_default();
                let counter = &mut aggregate.counter;
                counter.sample_count += 1;
                counter.sum += value;
            }
            CounterValue::Absolute(value) => {
                let aggregate = slot.entry(key).or_default();
                match &mut aggregate.counter_absolute {
                    Some(counter_absolute) => {
                        counter_absolute.current = value;
                    }
                    None => aggregate.counter_absolute = Some(CounterAbsolute { current: value }),
                }
            }
        },
        MetricData::Gauge(gauge_value) => {
            let aggregate = slot.entry(key).or_default();

            match &mut aggregate.gauge {
                Some(gauge) => {
                    gauge.current = gauge_value.update_value(gauge.current);
                    gauge.maximum = gauge.maximum.max(gauge.current);
                    gauge.minimum = gauge.minimum.min(gauge.current);
                }
                None => {
                    let value = gauge_value.update_value(0.0);
                    aggregate.gauge = Some(Gauge {
                        current: value,
                        maximum: value,
                        minimum: value,
                    });
                }
            }
        }
        MetricData::Histogram(value) => {
            let aggregate = slot.entry(key).or_default();
            *aggregate.histogram.entry(value).or_default() += 1;
        }
    }
}

fn accept_description(
    metrics_description: &mut HashMap<String, MetricConfig>,
    key_name: KeyName,
    unit: CloudwatchUnit,
) {
    let key = key_name.as_str().to_string();
    let unit = unit.0;
    metrics_description.insert(key, MetricConfig { unit });
}

struct Collector {
    metrics_data: BTreeMap<Timestamp, HashMap<Key, Aggregate>>,
    metrics_description: HashMap<String, MetricConfig>,
    config: CollectorConfig,
}

impl Collector {
    fn new(config: CollectorConfig) -> Self {
        Self {
            metrics_data: Default::default(),
            metrics_description: Default::default(),
            config,
        }
    }

    async fn accept_messages(&mut self, messages: impl Stream<Item = CollectorMessage>) {
        futures_util::pin_mut!(messages);
        while let Some(message) = messages.next().await {
            let result = async {
                match message {
                    CollectorMessage::Metric { key, data } => {
                        let timestamp = current_timestamp();
                        let slot = get_timeslot(&mut self.metrics_data, &self.config, timestamp);

                        // accept_description
                        // , &mut self.metrics_description
                        accept_datum(slot, key, data);

                        // `current_timestamp` can be pretty expensive when there are a lot of
                        // metrics so as long as we are immediately able to read more datums we
                        // assume that we can use the same timestamp for those, thereby amortizing
                        // the cost.
                        // 100 is arbitrarily chosen to get good a good amount of reuse without
                        // risking that this is always ready, thereby never updating the timestamp
                        for _ in 0..100 {
                            match future::lazy(|cx| messages.as_mut().poll_next(cx)).await {
                                Poll::Ready(Some(message)) => match message {
                                    CollectorMessage::Metric { key, data } => {
                                        accept_datum(slot, key, data)
                                    }
                                    CollectorMessage::SendBatch {
                                        send_all_before,
                                        emit_sender,
                                    } => {
                                        self.accept_send_batch(send_all_before, emit_sender)?;
                                        break;
                                    }
                                    CollectorMessage::DescribeMetric { key_name, unit } => {
                                        accept_description(
                                            &mut self.metrics_description,
                                            key_name,
                                            unit,
                                        )
                                    }
                                },
                                Poll::Ready(None) => return Ok(()),
                                Poll::Pending => {
                                    tokio::task::yield_now().await;
                                    break;
                                }
                            }
                        }
                        Ok(())
                    }
                    CollectorMessage::SendBatch {
                        send_all_before,
                        emit_sender,
                    } => self.accept_send_batch(send_all_before, emit_sender),
                    CollectorMessage::DescribeMetric { key_name, unit } => {
                        accept_description(&mut self.metrics_description, key_name, unit);
                        Ok(())
                    }
                }
            };
            if let Err(e) = result.await {
                warn!("Failed to accept message: {}", e);
            }
        }
    }

    async fn accept(&mut self, message: CollectorMessage) {
        self.accept_messages(stream::iter(Some(message))).await;
    }

    fn dimensions(&self, key: &Key) -> Vec<Dimension> {
        let mut dimensions_from_keys = key
            .labels()
            .filter(|l| !l.key().starts_with('@'))
            .map(|l| Dimension::builder().name(l.key()).value(l.value()).build())
            .peekable();

        let has_extra_dimensions = dimensions_from_keys.peek().is_some();

        let mut all_dims: Vec<_> = self
            .default_dimensions()
            .chain(dimensions_from_keys)
            .collect();

        if has_extra_dimensions {
            // Reverse order, so that the last value will override when we dedup
            all_dims.reverse();
            all_dims.sort_by(|a, b| a.name.cmp(&b.name));
            all_dims.dedup_by(|a, b| a.name == b.name);
        }
        all_dims.retain(|dim| !dim.value().unwrap_or("").is_empty());

        if all_dims.len() > MAX_CLOUDWATCH_DIMENSIONS {
            all_dims.truncate(MAX_CLOUDWATCH_DIMENSIONS);
            warn!(
                "Too many dimensions, taking only {}",
                MAX_CLOUDWATCH_DIMENSIONS
            );
        }

        all_dims
    }

    /// Sends a batch of the earliest collected metrics to CloudWatch
    ///
    /// # Params
    /// * send_all_before: All messages before this timestamp should be split off from the aggregation and
    /// sent to CloudWatch
    fn accept_send_batch(
        &mut self,
        send_all_before: Timestamp,
        emit_sender: mpsc::Sender<Vec<MetricDatum>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut range = self.metrics_data.split_off(&send_all_before);
        mem::swap(&mut range, &mut self.metrics_data);

        let mut metrics_batch = vec![];

        for (timestamp, stats_by_key) in range {
            let timestamp = datetime(time::UNIX_EPOCH + Duration::from_secs(timestamp));

            for (key, aggregate) in stats_by_key {
                let Aggregate {
                    counter,
                    counter_absolute,
                    gauge,
                    histogram,
                } = aggregate;
                let dimensions = self.dimensions(&key);

                let key_name = key.name();
                let unit = self
                    .metrics_description
                    .get(key_name)
                    .map(|config| config.unit.clone())
                    .or_else(|| {
                        let label_unit =
                            key.labels().find(|l| l.key() == "@unit").map(|l| l.value());
                        label_unit.map(|unit_str| StandardUnit::from(unit_str))
                        // Some(StandardUnit::from(label_unit))
                    });

                let stats_set_datum = &mut |stats_set, unit: &Option<StandardUnit>| {
                    MetricDatum::builder()
                        .set_dimensions(Some(dimensions.clone()))
                        .metric_name(key.name())
                        .timestamp(timestamp)
                        .storage_resolution(self.config.storage_resolution.as_secs() as i32)
                        .statistic_values(stats_set)
                        .set_unit(unit.clone())
                        .build()
                };

                if counter.sample_count > 0 {
                    let sum = counter.sum as f64;
                    let stats_set = StatisticSet::builder()
                        // We aren't interested in how many `counter!` calls we did so we put 1
                        // here to allow cloudwatch to display the average between this and any
                        // other instances posting to the same metric.
                        .sample_count(1.0)
                        .sum(sum)
                        // Max and min for a count can either be the sum or the max/min of the
                        // value passed to each `increment_counter` call.
                        //
                        // In the case where we only increment by `1` each call the latter makes
                        // min and max basically useless since the end result will leave both as `1`.
                        // In the case where we sum the count first before calling
                        // `increment_counter` we do lose some granularity as the latter would give
                        // a spread in min/max.
                        // However if that is an interesting metric it would often be
                        // better modeled as the gauge (measuring how much were processed in each
                        // batch).
                        //
                        // Therefor we opt to send the sum to give a measure of how many
                        // counts *this* metrics instance observed in this time period.
                        .maximum(sum)
                        .minimum(sum)
                        .build();

                    metrics_batch.push(stats_set_datum(
                        stats_set,
                        &unit.clone().or(Some(StandardUnit::Count)),
                    ));
                }

                if let Some(CounterAbsolute { current }) = counter_absolute {
                    let absolute_count = MetricDatum::builder()
                        .metric_name(key.name())
                        .set_dimensions(Some(dimensions.clone()))
                        .timestamp(timestamp)
                        .storage_resolution(self.config.storage_resolution.as_secs() as i32)
                        .value(current as f64)
                        .set_unit(unit.clone().or(Some(StandardUnit::Count)))
                        .build();
                    metrics_batch.push(absolute_count);
                }

                if let Some(Gauge {
                    current,
                    minimum,
                    maximum,
                }) = gauge
                {
                    // Gauges only submit the current value and the max and min
                    let statistic_set = StatisticSet::builder()
                        .sample_count(1.0)
                        .sum(current)
                        .maximum(maximum)
                        .minimum(minimum)
                        .build();

                    metrics_batch.push(stats_set_datum(statistic_set, &unit));
                }

                let histogram_datum =
                    &mut |Histogram { values, counts }, unit: &Option<StandardUnit>| {
                        MetricDatum::builder()
                            .metric_name(key.name())
                            .timestamp(timestamp)
                            .storage_resolution(self.config.storage_resolution.as_secs() as i32)
                            .set_dimensions(Some(dimensions.clone()))
                            .set_unit(unit.clone())
                            .set_values(Some(values))
                            .set_counts(Some(counts))
                            .build()
                    };

                if !histogram.is_empty() {
                    let histogram_data = &mut histogram.into_iter().map(|(k, v)| HistogramDatum {
                        value: f64::from(k),
                        count: v as f64,
                    });
                    loop {
                        let histogram = histogram_data.take(MAX_HISTOGRAM_VALUES).fold(
                            Histogram::default(),
                            |mut memo, datum| {
                                memo.values.push(datum.value);
                                memo.counts.push(datum.count);
                                memo
                            },
                        );
                        if histogram.values.is_empty() {
                            break;
                        };
                        metrics_batch.push(histogram_datum(histogram, &unit));
                    }
                }
            }
        }
        if !metrics_batch.is_empty() {
            emit_sender.try_send(metrics_batch)?;
        }
        Ok(())
    }

    fn default_dimensions(&self) -> impl Iterator<Item = Dimension> + '_ {
        self.config
            .default_dimensions
            .iter()
            .map(|(name, value)| Dimension::builder().name(name).value(value).build())
    }
}

#[cfg(test)]
mod tests {
    use metrics::Label;

    use super::*;

    use proptest::prelude::*;

    fn dim(name: &str, value: &str) -> Dimension {
        Dimension::builder().name(name).value(value).build()
    }

    #[test]
    fn time_key_should_truncate() {
        assert_eq!(time_key(370, Resolution::Second), 370);
        assert_eq!(time_key(370, Resolution::Minute), 360);
    }

    fn metrics() -> impl Strategy<Value = Vec<MetricDatum>> {
        let values = || {
            proptest::collection::vec(proptest::num::f64::ANY, 1..MAX_HISTOGRAM_VALUES)
                .prop_map(Some)
        };
        let timestamp = datetime(time::UNIX_EPOCH);
        let datum = (
            values(),
            values(),
            proptest::collection::vec(
                ("name", "value").prop_map(|(name, value)| dim(&name, &value)),
                1..6,
            )
            .prop_map(Some),
        )
            .prop_map(move |(counts, values, dimensions)| {
                MetricDatum::builder()
                    .set_counts(counts)
                    .set_values(values)
                    .set_dimensions(dimensions)
                    .metric_name("test")
                    .timestamp(timestamp)
                    .storage_resolution(1)
                    .statistic_values(StatisticSet::builder().build())
                    .unit(StandardUnit::Count)
                    .build()
            });

        proptest::collection::vec(datum, 1..100)
    }

    #[test]
    fn chunks_fit_in_cloudwatch_constraints() {
        proptest! {
            proptest::prelude::ProptestConfig { cases: 30, .. Default::default() },
            |(metrics in metrics())| {
                let mut total_chunks = 0;
                for metric_data in metrics_chunks(&metrics) {
                    assert!(metric_data.len() > 0 && metric_data.len() < MAX_CW_METRICS_PER_CALL, "Sending too many metrics per call: {}", metric_data.len());
                    let estimated_size = metric_data.iter().map(metric_size).sum::<usize>();
                    assert!(estimated_size < MAX_CW_METRICS_PUT_SIZE, "{} >= {}", estimated_size, MAX_CW_METRICS_PUT_SIZE);
                    total_chunks += metric_data.len();
                }
                assert_eq!(total_chunks, metrics.len());
            }
        }
    }

    #[test]
    fn should_override_default_dimensions() {
        let collector = Collector::new(CollectorConfig {
            default_dimensions: vec![
                ("second".to_owned(), "123".to_owned()),
                ("first".to_owned(), "initial-value".to_owned()),
            ]
            .into_iter()
            .collect(),
            storage_resolution: Resolution::Minute,
        });

        let key = Key::from_parts(
            "my-metric",
            vec![
                Label::new("zzz", "123"),
                Label::new("first", "override-value"),
                Label::new("aaa", "123"),
            ],
        );

        let actual = collector.dimensions(&key);
        let dim = |name, value| Dimension::builder().name(name).value(value).build();
        assert_eq!(
            actual,
            vec![
                dim("aaa", "123"),
                dim("first", "override-value"),
                dim("second", "123"),
                dim("zzz", "123")
            ],
        );
    }

    macro_rules! assert_between {
        ($x: expr, $min: expr, $max: expr, $(,)?) => {
            match ($x, $min, $max) {
                (x, min, max) => {
                    assert!(
                        x > min && x < max,
                        "{:?} is not in the range {:?}..{:?}",
                        x,
                        min,
                        max
                    );
                }
            }
        };
    }

    #[tokio::test]
    async fn jitter_interval_test() {
        let interval = Duration::from_millis(10);
        let start = tokio::time::Instant::now();
        let mut interval_stream = Box::pin(jitter_interval_at(start, interval));
        assert_eq!(interval_stream.next().await, Some(start));
        let x = interval_stream.next().await.unwrap();
        assert_between!(
            x,
            start.checked_add(interval / 2).unwrap(),
            start.checked_add(interval + interval / 2).unwrap(),
        );
    }

    /*  #[test]
    fn should_handle_nan_in_record_histogram() {
        let (sender, _receiver) = mpsc::channel(1);
        let recorder = RecorderHandle { sender };
        recorder.record_histogram(&Key::from_static_name("my_metric"), f64::NAN);
    }*/
}
