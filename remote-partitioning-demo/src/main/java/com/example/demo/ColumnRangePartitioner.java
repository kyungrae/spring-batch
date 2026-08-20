package com.example.demo;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Splits a table by a numeric column into {@code gridSize} contiguous ID ranges — this is
 * the "who is responsible for which slice of the data" logic Q1 was about.
 *
 * <p>
 * It queries {@code MIN(column)}/{@code MAX(column)}, divides that span into equal
 * ranges, and writes each range into a partition's {@link ExecutionContext} as
 * {@code minValue} / {@code maxValue}. The worker step then reads only
 * {@code WHERE column BETWEEN minValue
 * AND maxValue}, so each worker owns a disjoint slice. (Copied from
 * {@code org.springframework.batch.samples.common.ColumnRangePartitioner}.)
 */
public class ColumnRangePartitioner implements Partitioner {

	private JdbcOperations jdbcTemplate;

	private String table;

	private String column;

	public void setTable(String table) {
		this.table = table;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public void setDataSource(DataSource dataSource) {
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	}

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		int min = this.jdbcTemplate.queryForObject("SELECT MIN(" + this.column + ") from " + this.table, Integer.class);
		int max = this.jdbcTemplate.queryForObject("SELECT MAX(" + this.column + ") from " + this.table, Integer.class);
		int targetSize = (max - min) / gridSize + 1;

		Map<String, ExecutionContext> result = new HashMap<>();
		int number = 0;
		int start = min;
		int end = start + targetSize - 1;

		while (start <= max) {
			ExecutionContext value = new ExecutionContext();
			result.put("partition" + number, value);

			if (end >= max) {
				end = max;
			}
			value.putInt("minValue", start); // this partition owns [minValue, maxValue]
			value.putInt("maxValue", end);
			start += targetSize;
			end += targetSize;
			number++;
		}

		return result;
	}

}
