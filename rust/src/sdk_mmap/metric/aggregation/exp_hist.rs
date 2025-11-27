//! Exponential Histogram implementation
//!
//! This impelmentation is taken from opentelemetry-rust, as we'll be contributing this back there if any changes are made.

use core::f64;
use std::f64::consts::LOG2_E;
use std::sync::OnceLock;

pub(crate) const EXPO_MAX_SCALE: i8 = 20;
pub(crate) const EXPO_MIN_SCALE: i8 = -10;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BucketConfig {
    pub max_size: i32,
    pub max_scale: i8,
}

/// A single data point in an exponential histogram.
#[derive(Debug, PartialEq)]
struct ExpoHistogramDataPoint {
    max_size: i32,
    count: usize,
    min: f64,
    max: f64,
    sum: f64,
    scale: i8,
    pos_buckets: ExpoBuckets,
    neg_buckets: ExpoBuckets,
    zero_count: u64,
}

impl ExpoHistogramDataPoint {
    fn new(config: &BucketConfig) -> Self {
        ExpoHistogramDataPoint {
            max_size: config.max_size,
            count: 0,
            min: f64::MAX,
            max: f64::MIN,
            sum: 0.,
            scale: config.max_scale,
            pos_buckets: ExpoBuckets::default(),
            neg_buckets: ExpoBuckets::default(),
            zero_count: 0,
        }
    }
    /// Adds a new measurement to the histogram.
    ///
    /// It will rescale the buckets if needed.
    fn record(&mut self, v: f64) {
        self.count += 1;

        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
        self.sum += v;

        let abs_v = v.abs();

        if abs_v == 0.0 {
            self.zero_count += 1;
            return;
        }

        let mut bin = self.get_bin(abs_v);

        let v_is_negative = v < 0.;

        // If the new bin would make the counts larger than `max_scale`, we need to
        // downscale current measurements.
        let scale_delta = {
            let bucket = if v_is_negative {
                &self.neg_buckets
            } else {
                &self.pos_buckets
            };

            scale_change(
                self.max_size,
                bin,
                bucket.start_bin,
                bucket.counts.len() as i32,
            )
        };
        if scale_delta > 0 {
            if (self.scale - scale_delta as i8) < EXPO_MIN_SCALE {
                // With a scale of -10 there is only two buckets for the whole range of f64 values.
                // This can only happen if there is a max size of 1.

                // TODO - to check if this should be logged as an error if this is auto-recoverable.
                // otel_debug!(
                //     name: "ExponentialHistogramDataPoint.Scale.Underflow",
                //     current_scale = self.scale,
                //     scale_delta = scale_delta,
                //     max_size = self.max_size,
                //     min_scale = EXPO_MIN_SCALE,
                //     value = format!("{:?}", v),
                //     message = "The measurement will be dropped due to scale underflow. Check the histogram configuration"
                // );

                return;
            }
            // Downscale
            self.scale -= scale_delta as i8;
            self.pos_buckets.downscale(scale_delta);
            self.neg_buckets.downscale(scale_delta);

            bin = self.get_bin(abs_v);
        }

        if v_is_negative {
            self.neg_buckets.record(bin)
        } else {
            self.pos_buckets.record(bin)
        }
    }

    /// the bin `v` should be recorded into.
    fn get_bin(&self, v: f64) -> i32 {
        let (frac, exp) = frexp(v);
        if self.scale <= 0 {
            // With negative scale `frac` is always 1 power of two higher than we want.
            let mut correction = 1;
            if frac == 0.5 {
                // If `v` is an exact power of two, `frac` will be `0.5` and the exp
                // will be then be two higher than we want.
                correction = 2;
            }
            return (exp - correction) >> -self.scale;
        }
        (exp << self.scale) + (frac.ln() * scale_factors()[self.scale as usize]) as i32 - 1
    }
}

/// The magnitude of the scale change needed to fit bin in the bucket.
///
/// If no scale change is needed 0 is returned.
fn scale_change(max_size: i32, bin: i32, start_bin: i32, length: i32) -> u32 {
    if length == 0 {
        // No need to rescale if there are no buckets.
        return 0;
    }

    let mut low = start_bin;
    let mut high = bin;
    if start_bin >= bin {
        low = bin;
        high = start_bin + length - 1;
    }

    let mut count = 0u32;
    while high - low >= max_size {
        low >>= 1;
        high >>= 1;
        count += 1;

        if count > (EXPO_MAX_SCALE - EXPO_MIN_SCALE) as u32 {
            return count;
        }
    }

    count
}

// TODO - replace it with LazyLock once it is stable
static SCALE_FACTORS: OnceLock<[f64; 21]> = OnceLock::new();

/// returns constants used in calculating the logarithm index.
#[inline]
fn scale_factors() -> &'static [f64; 21] {
    SCALE_FACTORS.get_or_init(|| {
        [
            LOG2_E * 2f64.powi(0),
            LOG2_E * 2f64.powi(1),
            LOG2_E * 2f64.powi(2),
            LOG2_E * 2f64.powi(3),
            LOG2_E * 2f64.powi(4),
            LOG2_E * 2f64.powi(5),
            LOG2_E * 2f64.powi(6),
            LOG2_E * 2f64.powi(7),
            LOG2_E * 2f64.powi(8),
            LOG2_E * 2f64.powi(9),
            LOG2_E * 2f64.powi(10),
            LOG2_E * 2f64.powi(11),
            LOG2_E * 2f64.powi(12),
            LOG2_E * 2f64.powi(13),
            LOG2_E * 2f64.powi(14),
            LOG2_E * 2f64.powi(15),
            LOG2_E * 2f64.powi(16),
            LOG2_E * 2f64.powi(17),
            LOG2_E * 2f64.powi(18),
            LOG2_E * 2f64.powi(19),
            LOG2_E * 2f64.powi(20),
        ]
    })
}

/// Breaks the number into a normalized fraction and a base-2 exponent.
///
/// This impl is necessary as rust removed this functionality from std in
/// <https://github.com/rust-lang/rust/pull/41437>
#[inline(always)]
fn frexp(x: f64) -> (f64, i32) {
    let mut y = x.to_bits();
    let ee = ((y >> 52) & 0x7ff) as i32;

    if ee == 0 {
        if x != 0.0 {
            let x1p64 = f64::from_bits(0x43f0000000000000);
            let (x, e) = frexp(x * x1p64);
            return (x, e - 64);
        }
        return (x, 0);
    } else if ee == 0x7ff {
        return (x, 0);
    }

    let e = ee - 0x3fe;
    y &= 0x800fffffffffffff;
    y |= 0x3fe0000000000000;

    (f64::from_bits(y), e)
}

/// A set of buckets in an exponential histogram.
#[derive(Default, Debug, PartialEq)]
struct ExpoBuckets {
    start_bin: i32,
    counts: Vec<u64>,
}

impl ExpoBuckets {
    /// Increments the count for the given bin, and expands the buckets if needed.
    ///
    /// Size changes must be done before calling this function.
    fn record(&mut self, bin: i32) {
        if self.counts.is_empty() {
            self.counts = vec![1];
            self.start_bin = bin;
            return;
        }

        let end_bin = self.start_bin + self.counts.len() as i32 - 1;

        // if the new bin is inside the current range
        if bin >= self.start_bin && bin <= end_bin {
            self.counts[(bin - self.start_bin) as usize] += 1;
            return;
        }

        // if the new bin is before the current start, prepend the slots in `self.counts`
        if bin < self.start_bin {
            let mut zeroes = vec![0; (end_bin - bin + 1) as usize];
            let shift = (self.start_bin - bin) as usize;
            zeroes[shift..].copy_from_slice(&self.counts);
            self.counts = zeroes;
            self.counts[0] = 1;
            self.start_bin = bin;
        } else if bin > end_bin {
            // if the new bin is after the end, initialize the slots up to the new bin
            if ((bin - self.start_bin) as usize) < self.counts.capacity() {
                self.counts.resize((bin - self.start_bin + 1) as usize, 0);
                self.counts[(bin - self.start_bin) as usize] = 1;
                return;
            }

            self.counts.extend(
                std::iter::repeat(0).take((bin - self.start_bin) as usize - self.counts.len() + 1),
            );
            self.counts[(bin - self.start_bin) as usize] = 1
        }
    }

    /// Shrinks a bucket by a factor of 2*s.
    ///
    /// It will sum counts into the correct lower resolution bucket.
    fn downscale(&mut self, delta: u32) {
        // Example
        // delta = 2
        // original offset: -6
        // counts: [ 3,  1,  2,  3,  4,  5, 6, 7, 8, 9, 10]
        // bins:    -6  -5, -4, -3, -2, -1, 0, 1, 2, 3, 4
        // new bins:-2, -2, -1, -1, -1, -1, 0, 0, 0, 0, 1
        // new offset: -2
        // new counts: [4, 14, 30, 10]

        if self.counts.len() <= 1 || delta < 1 {
            self.start_bin >>= delta;
            return;
        }

        let steps = 1 << delta;
        let mut offset = self.start_bin % steps;
        offset = (offset + steps) % steps; // to make offset positive
        for i in 1..self.counts.len() {
            let idx = i + offset as usize;
            if idx % steps as usize == 0 {
                self.counts[idx / steps as usize] = self.counts[i];
                continue;
            }
            self.counts[idx / steps as usize] += self.counts[i];
        }

        let last_idx = (self.counts.len() as i32 - 1 + offset) / steps;
        self.counts = self.counts[..last_idx as usize + 1].to_vec();
        self.start_bin >>= delta;
    }

    fn to_otlp(
        &self,
    ) -> opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets {
        opentelemetry_proto::tonic::metrics::v1::exponential_histogram_data_point::Buckets {
            offset: self.start_bin,
            bucket_counts: self.counts.clone(),
        }
    }
}

impl super::Aggregation for ExpoHistogramDataPoint {
    fn join(
        &mut self,
        m: crate::sdk_mmap::data::Measurement,
    ) -> Result<(), crate::sdk_mmap::Error> {
        if let Some(v) = m.value {
            match v {
                super::super::data::measurement::Value::AsLong(lv) => self.record(lv as f64),
                super::super::data::measurement::Value::AsDouble(dv) => self.record(dv),
            }
        }
        Ok(())
    }

    fn collect(
        &self,
        id: &crate::sdk_mmap::metric::timeseries_id::TimeSeriesIdentity,
        ctx: &crate::sdk_mmap::metric::CollectionContext,
        cell: &mut opentelemetry_proto::tonic::metrics::v1::metric::Data,
    ) {
        if let opentelemetry_proto::tonic::metrics::v1::metric::Data::ExponentialHistogram(h) = cell
        {
            let point = opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint {
                attributes: id.to_otlp_attributes(),
                start_time_unix_nano: ctx.start_unix_nano,
                time_unix_nano: ctx.current_unix_nano,
                count: self.count as u64,
                sum: Some(self.sum),
                scale: self.scale as i32,
                zero_count: self.zero_count,
                positive: Some(self.pos_buckets.to_otlp()),
                negative: Some(self.neg_buckets.to_otlp()),
                flags: 0,
                // TODO - support exemplars
                exemplars: vec![],
                min: Some(self.min),
                max: Some(self.max),
                zero_threshold: 0.0,
            };
            h.data_points.push(point);
        }
    }
}

impl super::AggregationConfig for BucketConfig {
    fn new_aggregation(&self) -> Box<dyn super::Aggregation> {
        Box::new(ExpoHistogramDataPoint::new(self))
    }

    fn new_collection_data(&self) -> Option<opentelemetry_proto::tonic::metrics::v1::metric::Data> {
        Some(
            opentelemetry_proto::tonic::metrics::v1::metric::Data::ExponentialHistogram(
                opentelemetry_proto::tonic::metrics::v1::ExponentialHistogram {
                    data_points: Vec::new(),
                    aggregation_temporality:
                        opentelemetry_proto::tonic::metrics::v1::AggregationTemporality::Cumulative
                            .into(),
                },
            ),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // const CARDINALITY_LIMIT_DEFAULT: usize = 2000;

    #[test]
    fn run_data_point_record() {
        struct TestCase {
            max_size: i32,
            values: Vec<f64>,
            expected_buckets: ExpoBuckets,
            expected_scale: i8,
        }
        let test_cases: Vec<TestCase> = vec![
            TestCase {
                max_size: 4,
                values: vec![2, 4, 1].into_iter().map(Into::into).collect(),
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![1, 1, 1],
                },
                expected_scale: 0,
            },
            TestCase {
                max_size: 4,
                values: vec![4, 4, 4, 2, 16, 1]
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![1, 4, 1],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![1, 2, 4].into_iter().map(Into::into).collect(),
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![1, 2],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![1, 4, 2].into_iter().map(Into::into).collect(),
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![1, 2],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![2, 4, 1].into_iter().map(Into::into).collect(),
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![1, 2],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![2, 1, 4].into_iter().map(Into::into).collect(),
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![1, 2],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![4, 1, 2].into_iter().map(Into::into).collect(),
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![1, 2],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![4, 2, 1].into_iter().map(Into::into).collect(),
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![1, 2],
                },
                expected_scale: -1,
            },
        ];

        for test in test_cases {
            let mut dp = ExpoHistogramDataPoint::new(&BucketConfig {
                max_size: test.max_size,
                max_scale: 20,
            });
            for v in test.values {
                dp.record(v);
                dp.record(-v);
            }

            assert_eq!(test.expected_buckets, dp.pos_buckets, "positive buckets");
            assert_eq!(test.expected_buckets, dp.neg_buckets, "negative buckets");
            assert_eq!(test.expected_scale, dp.scale, "scale");
        }
    }

    #[test]
    fn run_data_point_record_f64() {
        struct TestCase {
            max_size: i32,
            values: Vec<f64>,
            expected_buckets: ExpoBuckets,
            expected_scale: i8,
        }

        let test_cases = vec![
            TestCase {
                max_size: 4,
                values: vec![2.0, 2.0, 2.0, 1.0, 8.0, 0.5],
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![2, 3, 1],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![1.0, 0.5, 2.0],
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![2, 1],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![1.0, 2.0, 0.5],
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![2, 1],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![2.0, 0.5, 1.0],
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![2, 1],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![2.0, 1.0, 0.5],
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![2, 1],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![0.5, 1.0, 2.0],
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![2, 1],
                },
                expected_scale: -1,
            },
            TestCase {
                max_size: 2,
                values: vec![0.5, 2.0, 1.0],
                expected_buckets: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![2, 1],
                },
                expected_scale: -1,
            },
        ];
        for test in test_cases {
            let mut dp = ExpoHistogramDataPoint::new(&BucketConfig {
                max_size: test.max_size,
                max_scale: 20,
            });
            for v in test.values {
                dp.record(v);
                dp.record(-v);
            }

            assert_eq!(test.expected_buckets, dp.pos_buckets);
            assert_eq!(test.expected_buckets, dp.neg_buckets);
            assert_eq!(test.expected_scale, dp.scale);
        }
    }

    #[test]
    fn data_point_record_limits() {
        // These bins are calculated from the following formula:
        // floor( log2( value) * 2^20 ) using an arbitrary precision calculator.

        let cfg = BucketConfig {
            max_size: 4,
            max_scale: 20,
        };
        let mut fdp = ExpoHistogramDataPoint::new(&cfg);
        fdp.record(f64::MAX);

        assert_eq!(
            fdp.pos_buckets.start_bin, 1073741823,
            "start bin does not match for large f64 values",
        );

        let mut fdp = ExpoHistogramDataPoint::new(&cfg);
        fdp.record(f64::MIN_POSITIVE);

        assert_eq!(
            fdp.pos_buckets.start_bin, -1071644673,
            "start bin does not match for small positive values",
        );

        let mut idp = ExpoHistogramDataPoint::new(&cfg);
        idp.record(i64::MAX as f64);

        assert_eq!(
            idp.pos_buckets.start_bin, 66060287,
            "start bin does not match for max i64 values",
        );
    }

    #[test]
    fn expo_bucket_downscale() {
        struct TestCase {
            name: &'static str,
            bucket: ExpoBuckets,
            scale: i8,
            want: ExpoBuckets,
        }

        let test_cases = vec![
            TestCase {
                name: "Empty bucket",
                bucket: ExpoBuckets {
                    start_bin: 0,
                    counts: vec![],
                },
                scale: 3,
                want: ExpoBuckets {
                    start_bin: 0,
                    counts: vec![],
                },
            },
            TestCase {
                name: "1 size bucket",
                bucket: ExpoBuckets {
                    start_bin: 50,
                    counts: vec![7],
                },
                scale: 4,
                want: ExpoBuckets {
                    start_bin: 3,
                    counts: vec![7],
                },
            },
            TestCase {
                name: "zero scale",
                bucket: ExpoBuckets {
                    start_bin: 50,
                    counts: vec![7, 5],
                },
                scale: 0,
                want: ExpoBuckets {
                    start_bin: 50,
                    counts: vec![7, 5],
                },
            },
            TestCase {
                name: "aligned bucket scale 1",
                bucket: ExpoBuckets {
                    start_bin: 0,
                    counts: vec![1, 2, 3, 4, 5, 6],
                },
                scale: 1,
                want: ExpoBuckets {
                    start_bin: 0,
                    counts: vec![3, 7, 11],
                },
            },
            TestCase {
                name: "aligned bucket scale 2",
                bucket: ExpoBuckets {
                    start_bin: 0,
                    counts: vec![1, 2, 3, 4, 5, 6],
                },
                scale: 2,
                want: ExpoBuckets {
                    start_bin: 0,
                    counts: vec![10, 11],
                },
            },
            TestCase {
                name: "aligned bucket scale 3",
                bucket: ExpoBuckets {
                    start_bin: 0,
                    counts: vec![1, 2, 3, 4, 5, 6],
                },
                scale: 3,
                want: ExpoBuckets {
                    start_bin: 0,
                    counts: vec![21],
                },
            },
            TestCase {
                name: "unaligned bucket scale 1",
                bucket: ExpoBuckets {
                    start_bin: 5,
                    counts: vec![1, 2, 3, 4, 5, 6],
                }, // This is equivalent to [0,0,0,0,0,1,2,3,4,5,6]
                scale: 1,
                want: ExpoBuckets {
                    start_bin: 2,
                    counts: vec![1, 5, 9, 6],
                }, // This is equivalent to [0,0,1,5,9,6]
            },
            TestCase {
                name: "unaligned bucket scale 2",
                bucket: ExpoBuckets {
                    start_bin: 7,
                    counts: vec![1, 2, 3, 4, 5, 6],
                }, // This is equivalent to [0,0,0,0,0,0,0,1,2,3,4,5,6]
                scale: 2,
                want: ExpoBuckets {
                    start_bin: 1,
                    counts: vec![1, 14, 6],
                }, // This is equivalent to [0,1,14,6]
            },
            TestCase {
                name: "unaligned bucket scale 3",
                bucket: ExpoBuckets {
                    start_bin: 3,
                    counts: vec![1, 2, 3, 4, 5, 6],
                }, // This is equivalent to [0,0,0,1,2,3,4,5,6]
                scale: 3,
                want: ExpoBuckets {
                    start_bin: 0,
                    counts: vec![15, 6],
                }, // This is equivalent to [0,15,6]
            },
            TestCase {
                name: "unaligned bucket scale 1",
                bucket: ExpoBuckets {
                    start_bin: 1,
                    counts: vec![1, 0, 1],
                },
                scale: 1,
                want: ExpoBuckets {
                    start_bin: 0,
                    counts: vec![1, 1],
                },
            },
            TestCase {
                name: "negative start_bin",
                bucket: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![1, 0, 3],
                },
                scale: 1,
                want: ExpoBuckets {
                    start_bin: -1,
                    counts: vec![1, 3],
                },
            },
            TestCase {
                name: "negative start_bin 2",
                bucket: ExpoBuckets {
                    start_bin: -4,
                    counts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                },
                scale: 1,
                want: ExpoBuckets {
                    start_bin: -2,
                    counts: vec![3, 7, 11, 15, 19],
                },
            },
        ];
        for mut test in test_cases {
            test.bucket.downscale(test.scale as u32);
            assert_eq!(test.want, test.bucket, "{}", test.name);
        }
    }

    #[test]
    fn expo_bucket_record() {
        struct TestCase {
            name: &'static str,
            bucket: ExpoBuckets,
            bin: i32,
            want: ExpoBuckets,
        }

        let test_cases = vec![
            TestCase {
                name: "Empty bucket creates first count",
                bucket: ExpoBuckets {
                    start_bin: 0,
                    counts: vec![],
                },
                bin: -5,
                want: ExpoBuckets {
                    start_bin: -5,
                    counts: vec![1],
                },
            },
            TestCase {
                name: "Bin is in the bucket",
                bucket: ExpoBuckets {
                    start_bin: 3,
                    counts: vec![1, 2, 3, 4, 5, 6],
                },
                bin: 5,
                want: ExpoBuckets {
                    start_bin: 3,
                    counts: vec![1, 2, 4, 4, 5, 6],
                },
            },
            TestCase {
                name: "Bin is before the start of the bucket",
                bucket: ExpoBuckets {
                    start_bin: 1,
                    counts: vec![1, 2, 3, 4, 5, 6],
                },
                bin: -2,
                want: ExpoBuckets {
                    start_bin: -2,
                    counts: vec![1, 0, 0, 1, 2, 3, 4, 5, 6],
                },
            },
            TestCase {
                name: "Bin is after the end of the bucket",
                bucket: ExpoBuckets {
                    start_bin: -2,
                    counts: vec![1, 2, 3, 4, 5, 6],
                },
                bin: 4,
                want: ExpoBuckets {
                    start_bin: -2,
                    counts: vec![1, 2, 3, 4, 5, 6, 1],
                },
            },
        ];

        for mut test in test_cases {
            test.bucket.record(test.bin);
            assert_eq!(test.want, test.bucket, "{}", test.name);
        }
    }

    #[test]
    fn scale_change_rescaling() {
        struct Args {
            bin: i32,
            start_bin: i32,
            length: i32,
            max_size: i32,
        }
        struct TestCase {
            name: &'static str,
            args: Args,
            want: u32,
        }
        let test_cases = vec![
            TestCase {
                name: "if length is 0, no rescale is needed",
                // [] -> [5] length 1
                args: Args {
                    bin: 5,
                    start_bin: 0,
                    length: 0,
                    max_size: 4,
                },
                want: 0,
            },
            TestCase {
                name: "if bin is between start, and the end, no rescale needed",
                // [-1, ..., 8] length 10 -> [-1, ..., 5, ..., 8] length 10
                args: Args {
                    bin: 5,
                    start_bin: -1,
                    length: 10,
                    max_size: 20,
                },
                want: 0,
            },
            TestCase {
                name: "if [bin,... end].len() > max_size, rescale needed",
                // [8,9,10] length 3 -> [5, ..., 10] length 6
                args: Args {
                    bin: 5,
                    start_bin: 8,
                    length: 3,
                    max_size: 5,
                },
                want: 1,
            },
            TestCase {
                name: "if [start, ..., bin].len() > max_size, rescale needed",
                // [2,3,4] length 3 -> [2, ..., 7] length 6
                args: Args {
                    bin: 7,
                    start_bin: 2,
                    length: 3,
                    max_size: 5,
                },
                want: 1,
            },
            TestCase {
                name: "if [start, ..., bin].len() > max_size, rescale needed",
                // [2,3,4] length 3 -> [2, ..., 7] length 12
                args: Args {
                    bin: 13,
                    start_bin: 2,
                    length: 3,
                    max_size: 5,
                },
                want: 2,
            },
            TestCase {
                name: "It should not hang if it will never be able to rescale",
                args: Args {
                    bin: 1,
                    start_bin: -1,
                    length: 1,
                    max_size: 1,
                },
                want: 31,
            },
        ];

        for test in test_cases {
            let got = scale_change(
                test.args.max_size,
                test.args.bin,
                test.args.start_bin,
                test.args.length,
            );
            assert_eq!(got, test.want, "incorrect scale change, {}", test.name);
        }
    }

    #[test]
    fn sub_normal() {
        let want = ExpoHistogramDataPoint {
            max_size: 4,
            count: 3,
            min: f64::MIN_POSITIVE,
            max: f64::MIN_POSITIVE,
            sum: 3.0 * f64::MIN_POSITIVE,

            scale: 20,
            pos_buckets: ExpoBuckets {
                start_bin: -1071644673,
                counts: vec![3],
            },
            neg_buckets: ExpoBuckets {
                start_bin: 0,
                counts: vec![],
            },
            zero_count: 0,
        };

        let mut ehdp = ExpoHistogramDataPoint::new(&BucketConfig {
            max_size: 4,
            max_scale: 20,
        });
        ehdp.record(f64::MIN_POSITIVE);
        ehdp.record(f64::MIN_POSITIVE);
        ehdp.record(f64::MIN_POSITIVE);

        assert_eq!(want, ehdp);
    }
}
