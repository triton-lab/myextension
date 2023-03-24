export interface IBatchJobItem {
  name: string;
  job_id: string;
  instance_type: string;
  timestamp: Date;
  extra: string;
  status: string;
  console_output: string;
}
