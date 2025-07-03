const settings = require("../settings.js");
const { Client } = require("cassandra-driver");
const mysql = require("mysql2/promise");

async function run() {
  const con = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '1234',
    database: 'employees'
  });

  const client = new Client({
    cloud: { secureConnectBundle: "secure-connect-cassandra-test.zip" },
    credentials: {
      username: settings.clientId,
      password: settings.secret,
    },
  });
  await client.connect();

  await client.execute("DROP TABLE IF EXISTS aulas.media_salarial_employee_por_dept");
  await client.execute(`
  CREATE TABLE aulas.media_salarial_employee_por_dept (
    dept_name TEXT PRIMARY KEY,
    total_employees INT,
    media_salario DECIMAL
  );
  `);

  const sql = `
    SELECT d.dept_name, COUNT(DISTINCT de.emp_no) AS total_employees, ROUND(AVG(s.salary), 2) AS media_salario FROM dept_emp AS de
    JOIN departments AS d ON de.dept_no = d.dept_no
    JOIN (
      SELECT emp_no, salary
      FROM salaries AS s1
      WHERE (s1.emp_no, s1.from_date) IN (
        SELECT emp_no, MAX(from_date)
        FROM salaries
        GROUP BY emp_no
      )
    ) AS s ON de.emp_no = s.emp_no
    WHERE CURDATE() BETWEEN de.from_date AND de.to_date
    GROUP BY d.dept_name;
  `;
  const [rows] = await con.execute(sql);

  const insertQuery = `
    INSERT INTO aulas.media_salarial_employee_por_dept
    (dept_name, total_employees, media_salario)
    VALUES (?, ?, ?)
  `;

  for (const r of rows) {
    await client.execute(insertQuery, [
      r.dept_name,
      r.total_employees,
      r.media_salario,
    ], { prepare: true });
    console.log(`Inserido depto ${r.dept_name} — ${r.total_employees} empregados, média ${r.media_salario}`);
  }

  console.log("Média salarial por departamento populada com sucesso.");
  await client.shutdown();
  await con.end();
}

run().catch(err => console.error(err));