CREATE TABLE IF NOT EXISTS smoke_probe (
  id INT PRIMARY KEY,
  label TEXT NOT NULL
);
INSERT INTO smoke_probe (id, label) VALUES (1, 'kyuubi-system-tests')
  ON CONFLICT (id) DO NOTHING;
