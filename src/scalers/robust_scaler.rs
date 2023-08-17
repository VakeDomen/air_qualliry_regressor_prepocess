pub struct RobustScaler {
    median: f32,
    iqr: f32,
}

impl RobustScaler {
    pub fn new(data: &[f32]) -> Self {
        let mut sorted_data = data.to_vec();
        sorted_data.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let n = sorted_data.len();
        let median = if n % 2 == 0 {
            (sorted_data[n / 2 - 1] + sorted_data[n / 2]) / 2.0
        } else {
            sorted_data[n / 2]
        };

        let q1 = if n % 4 == 0 {
            (sorted_data[n / 4 - 1] + sorted_data[n / 4]) / 2.0
        } else {
            sorted_data[n / 4]
        };

        let q3 = if n % 4 == 0 {
            (sorted_data[3 * n / 4 - 1] + sorted_data[3 * n / 4]) / 2.0
        } else {
            sorted_data[3 * n / 4]
        };

        let iqr = q3 - q1;
        
        RobustScaler { median, iqr }
    }

    pub fn transform(&self, value: f32) -> f32 {
        (value - self.median) / self.iqr
    }
}
