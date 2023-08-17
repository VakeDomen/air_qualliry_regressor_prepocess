
pub struct StandardScaler {
    mean: f32,
    std_dev: f32,
}

impl StandardScaler {
    pub fn new(data: &[f32]) -> Self {
        let mean = data.iter().sum::<f32>() / (data.len() as f32);
        let var = data.iter().map(|&value| (value - mean).powi(2)).sum::<f32>() / (data.len() as f32);
        let std_dev = var.sqrt();
        
        StandardScaler { mean, std_dev }
    }

    pub fn transform(&self, value: f32) -> f32 {
        (value - self.mean) / self.std_dev
    }
}