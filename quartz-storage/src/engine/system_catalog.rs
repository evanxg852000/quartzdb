use hashbrown::HashMap;
use parking_lot::RwLock;
use quartz_common::{LogStream, Object, ObjectId, TimeSeries};

const SYSTEM_CATALOG_FILE: &str = "manifest.info";


#[derive(Debug)]
pub struct SystemCatalog {
    objects: RwLock<HashMap<ObjectId, Object>>,
    file: SystemCatalogFile,
}

impl SystemCatalog {
    pub fn open(directory: impl AsRef<Path>) -> Self {
        let mut system_catalog_file = SystemCatalogFile::open(directory);
        let objects = file.load();
        Self {
            objects: RwLock::new(objects),
            file: system_catalog_file,
        }
    }

    pub fn add_time_series(&self, time_series: TimeSeries) {
        let object_id = time_series.time_series_id;
        let mut objects_guard = self.objects.write();
        match objects_guard.get(&object_id) {
            Some(Object::TimeSeries(_)) => { 
                //TODO: check if equal or reject
                // Time series already exists, do nothing
                return;
            }
            Some(Object::LogStream(_)) => {
                panic!("A log stream with the same ID `{}` already exists", object_id);
            }
            None => {
                objects_guard.insert(object_id, Object::TimeSeries(time_series));
            }
        }
    }

    pub fn add_log_stream(&self, log_stream: LogStream) {
        let object_id = log_stream.log_stream_id;
        let mut objects_guard = self.objects.write();
        match objects_guard.get(&object_id) {
            Some(Object::TimeSeries(_)) => { 
                panic!("A time series with the same ID `{}` already exists", object_id);
            }
            Some(Object::LogStream(_)) => {
                //TODO: check if equal or reject
                // Log stream already exists, do nothing
                return;
            }
            None => {
                objects_guard.insert(object_id, Object::LogStream(log_stream));
            }
        }

        self.log_streams.insert(log_stream.log_stream_id, log_stream);
    }

    pub fn get_time_series(&self, id: TimeSeriesId) -> Option<&TimeSeries> {
        match self.objects.read().get(&id) {
            Some(Object::TimeSeries(ts)) => Some(ts),
            Some(Object::LogStream(_)) => None,
            None => None,
        }
    }

    pub fn get_log_stream(&self, id: LogStreamId) -> Option<&LogStream> {
        match self.objects.read().get(&id) {
            Some(Object::TimeSeries(_)) => None,
            Some(Object::LogStream(ls)) => Some(ls),
            None => None,
        }
    }
    
}

struct SystemCatalogFile {
    file: File,
}

impl SystemCatalogFile {
    pub fn open(directory: impl AsRef<Path>) -> Self {
        let log_file_path = directory.as_ref().join(SYSTEM_CATALOG_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(log_file_path)
            .unwrap();
        Self { file }
    }

    pub fn load(&mut self) -> HashMap<ObjectId, Object> {
        let mut objects = HashMap::new();
        let mut reader = BufReader::new(&self.file);
        while let Ok(object) = bincode::decode_from_reader::<Object>(reader, bincode::config::standard()) {
            objects.insert(object.get_id(), object);
        }
        objects
    }

    pub fn write(&mut self, object: &Object) {
        let mut writer = BufWriter::new(&self.file);
        bincode::encode_into_writer(object, writer, bincode::config::standard()).unwrap();
        writer.flush().unwrap();
    }
}

