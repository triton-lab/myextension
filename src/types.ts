export interface IBatchJobItem {
  name: string;
  file_path: string;
  job_id: string;
  instance_type: string;
  ensured_storage_size: number;
  timestamp: string;
  shared_dir: string;
  extra: string;
  status: string;
  console_output: string;
}
