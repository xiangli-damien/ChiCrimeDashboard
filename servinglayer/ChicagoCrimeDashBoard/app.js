'use strict';

require('dotenv').config();
const express = require('express');
const mustache = require('mustache');
const fs = require('fs');
const path = require('path');
const hbase = require('hbase');

const app = express();

// Configuration
const port = Number(process.argv[2]) || 3000;
const hbaseUrlStr = process.argv[3] || process.env.HBASE_URL;

if (!hbaseUrlStr) {
	console.error("No HBase URL provided. Please set HBASE_URL in .env or provide as argument.");
	process.exit(1);
}

let hbaseUrl;
try {
	hbaseUrl = new URL(hbaseUrlStr);
} catch (e) {
	console.error("Invalid HBase URL:", hbaseUrlStr, e);
	process.exit(1);
}

// Initialize HBase client
const hclient = hbase({
	host: hbaseUrl.hostname,
	path: hbaseUrl.pathname || "/",
	port: hbaseUrl.port || (hbaseUrl.protocol === 'http:' ? 80 : 443),
	protocol: hbaseUrl.protocol.slice(0, -1),
	encoding: 'latin1',
	auth: process.env.HBASE_AUTH
});

// Set static files directory
app.use(express.static(path.join(__dirname, 'public')));

// Parse URL-encoded form data
app.use(express.urlencoded({ extended: true }));

// Pre-load templates
const indexTemplate = fs.readFileSync(path.join(__dirname, 'views', 'index.mustache')).toString();
const dashboardTemplate = fs.readFileSync(path.join(__dirname, 'views', 'dashboard.mustache')).toString();

// Helper function to parse Yes/No/Both inputs
function parseCondition(value) {
	value = value.trim().toLowerCase();
	if (value === 'yes' || value === '1') return ['1'];
	if (value === 'no' || value === '0') return ['0'];
	if (value === 'both') return ['0', '1'];
	return ['0', '1']; // Default to both if invalid input
}

// Fetch latest crimes for the warning banner
async function fetchLatestCrimes() {
	const rowkeys = [
		"20241130_20_1733769152683",
		"20241130_22_1733769153624",
		"20241130_46_1733769153614"
	];

	const latestCrimes = [];
	for (const rowkey of rowkeys) {
		const shortRowkey = rowkey.split('_').slice(0, 3).join('_'); // Trim the timestamp
		try {
			const cells = await new Promise((resolve, reject) =>
				hclient.table('xli0808_speed_latest_crimes_table').row(shortRowkey).get((err, cells) => {
					if (err) reject(err);
					else resolve(cells);
				})
			);
			const crime = {
				date: cells.find(cell => cell.column === 'info:date')?.['$'],
				latitude: parseFloat(cells.find(cell => cell.column === 'info:latitude')?.['$']),
				longitude: parseFloat(cells.find(cell => cell.column === 'info:longitude')?.['$']),
				type: cells.find(cell => cell.column === 'info:primary_type')?.['$']
			};
			if (crime.latitude && crime.longitude) latestCrimes.push(crime);
		} catch (err) {
			console.error(`Error fetching latest crime for rowkey ${rowkey}:`, err);
		}
	}
	return latestCrimes;
}

// Home route: show query form
app.get('/', async (req, res) => {
	const latestCrimes = await fetchLatestCrimes();
	const html = mustache.render(indexTemplate, {
		latestCrimes,
		latestCrimesJson: JSON.stringify(latestCrimes)
	});
	res.send(html);
});

// Query route: fetch data from HBase
app.post('/query', async (req, res) => {
	const yearmonth = (req.body.yearmonth || '').trim();
	const arrestInput = (req.body.arrest || '').trim();
	const domesticInput = (req.body.domestic || '').trim();

	if (!yearmonth || !arrestInput || !domesticInput) {
		return res.send("Please provide Year-Month (YYYYMM), Arrest, and Domestic values.");
	}

	const arrestValues = parseCondition(arrestInput);
	const domesticValues = parseCondition(domesticInput);

	const table = hclient.table('xli0808_project2_crime_monthly_hbase');
	const districtData = {};

	for (const district of Array.from({ length: 24 }, (_, i) => (i + 1).toString()).concat(['31'])) {
		let totalCrimeCount = 0;
		for (const aVal of arrestValues) {
			for (const dVal of domesticValues) {
				const rowkey = `${yearmonth}#${aVal}#${dVal}#${district}`;
				try {
					const cells = await new Promise((resolve, reject) =>
						table.row(rowkey).get((err, cells) => (err ? reject(err) : resolve(cells)))
					);
					const crimeCount = cells && cells.find(cell => cell.column === 'cf:crime_count')
						? parseInt(cells.find(cell => cell.column === 'cf:crime_count')['$'], 10)
						: 0;
					totalCrimeCount += (isNaN(crimeCount) ? 0 : crimeCount);
				} catch (err) {
					if (err.code !== 404) {
						console.error(`Error fetching data for district ${district} with arrest=${aVal},domestic=${dVal}:`, err);
					}
				}
			}
		}
		districtData[district] = totalCrimeCount;
	}

	const arrestDisplay = arrestValues.length === 2 ? 'Both' : arrestValues[0] === '1' ? 'Yes' : 'No';
	const domesticDisplay = domesticValues.length === 2 ? 'Both' : domesticValues[0] === '1' ? 'Yes' : 'No';

	const sortedData = Object.entries(districtData)
		.map(([district, count]) => ({ district, count }))
		.sort((a, b) => b.count - a.count);

	const latestCrimes = await fetchLatestCrimes();
	const data = {
		yearmonth,
		arrest: arrestDisplay,
		domestic: domesticDisplay,
		districts: sortedData,
		latestCrimes,
		latestCrimesJson: JSON.stringify(latestCrimes),
		jsonData: JSON.stringify(sortedData)
	};

	const html = mustache.render(dashboardTemplate, data);
	res.send(html);
});

// Start the server
app.listen(port, () => {
	console.log(`Server running on port ${port}`);
});
