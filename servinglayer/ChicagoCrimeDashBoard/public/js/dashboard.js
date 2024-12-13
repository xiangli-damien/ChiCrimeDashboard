async function loadDataAndRender(start, end, districts) {
    console.log("Requesting data:", `/api/data?start=${start}&end=${end}&districts=${districts}`);
    const response = await fetch(`/api/data?start=${start}&end=${end}&districts=${districts}`);
    if (!response.ok) {
        console.error("Network response was not ok:", response.statusText);
        return;
    }
    const jsonData = await response.json();

    console.log("Data received from API:", jsonData);

    if (jsonData.error) {
        console.error(jsonData.error);
        return;
    }

    const timeseries = jsonData.timeseries;
    const byDistrict = jsonData.byDistrict;

    if (!timeseries || timeseries.length === 0) {
        console.warn("No timeseries data available.");
        d3.select("#line_chart").append("text")
            .attr("x", 300)
            .attr("y", 200)
            .attr("text-anchor", "middle")
            .text("No timeseries data available.");
        return;
    }

    if (!byDistrict || Object.keys(byDistrict).length === 0) {
        console.warn("No byDistrict data available.");
        d3.select("#bar_chart").append("text")
            .attr("x", 300)
            .attr("y", 200)
            .attr("text-anchor", "middle")
            .text("No district data available.");
        return;
    }

    renderLineChart(timeseries);
    renderMonthSelector(byDistrict);

    let firstMonth = Object.keys(byDistrict)[0];
    renderBarChart(byDistrict, firstMonth);

    document.getElementById("month_selector").addEventListener("change", (e) => {
        renderBarChart(byDistrict, e.target.value);
    });
}

function renderBarChart(byDistrict, selectedMonth) {
    console.log("Rendering bar chart for month:", selectedMonth);
    const data = [];
    for (let dist in byDistrict[selectedMonth]) {
        data.push({ district: dist, count: byDistrict[selectedMonth][dist] });
    }

    if (!data || data.length === 0) {
        d3.select("#bar_chart").append("text")
            .attr("x", 300)
            .attr("y", 200)
            .attr("text-anchor", "middle")
            .text("No data available for the selected month.");
        return;
    }

    const svg = d3.select("#bar_chart");
    svg.selectAll("*").remove();

    const margin = { top: 40, right: 40, bottom: 60, left: 60 };
    const width = +svg.attr("width") - margin.left - margin.right;
    const height = +svg.attr("height") - margin.top - margin.bottom;
    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const x = d3.scaleBand()
        .domain(data.map(d => d.district))
        .range([0, width])
        .padding(0.1);

    const y = d3.scaleLinear()
        .domain([0, d3.max(data, d => d.count) || 1]) // 防止全 0 数据导致比例尺崩溃
        .range([height, 0]);

    g.append("g")
        .attr("transform", `translate(0,${height})`)
        .call(d3.axisBottom(x));

    g.append("g").call(d3.axisLeft(y));

    g.selectAll(".bar")
        .data(data)
        .enter()
        .append("rect")
        .attr("class", "bar")
        .attr("x", d => x(d.district))
        .attr("y", d => y(d.count))
        .attr("width", x.bandwidth())
        .attr("height", d => height - y(d.count))
        .attr("fill", d => (d.count > 50 ? "red" : "orange")); // 根据数据动态着色

    g.selectAll(".label")
        .data(data)
        .enter()
        .append("text")
        .attr("x", d => x(d.district) + x.bandwidth() / 2)
        .attr("y", d => y(d.count) - 5)
        .attr("text-anchor", "middle")
        .style("font-size", "10px")
        .text(d => d.count);

    g.append("text")
        .attr("x", width / 2)
        .attr("y", -10)
        .attr("text-anchor", "middle")
        .style("font-size", "16px")
        .text(`Crimes by District for ${selectedMonth}`);
}
