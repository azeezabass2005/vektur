use std::rc::Rc;

use crate::errors::QueryError;
use crate::{ColumnVector, DataSource, RecordBatch, Schema, ScalarValue};

use super::eval::PhysicalExpr;

pub trait PhysicalPlan {
    fn schema(&self) -> &Schema;
    fn execute(&self) -> Box<dyn Iterator<Item = Result<RecordBatch, QueryError>> + '_>;
    fn children(&self) -> Vec<&dyn PhysicalPlan>;
}

/// Reads batches from a data source. Leaf node — no children.
pub struct ScanExec {
    pub source: Rc<dyn DataSource>,
    pub schema: Schema,
}

impl PhysicalPlan for ScanExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Box<dyn Iterator<Item = Result<RecordBatch, QueryError>> + '_> {
        self.source.scan()
    }

    fn children(&self) -> Vec<&dyn PhysicalPlan> {
        vec![]
    }
}

/// Keeps only rows where the predicate evaluates to true.
pub struct FilterExec {
    pub input: Box<dyn PhysicalPlan>,
    pub predicate: Box<dyn PhysicalExpr>,
}

impl PhysicalPlan for FilterExec {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn execute(&self) -> Box<dyn Iterator<Item = Result<RecordBatch, QueryError>> + '_> {
        let input_iter = self.input.execute();
        let predicate = &self.predicate;

        Box::new(input_iter.map(move |batch_result| {
            let batch = batch_result?;
            let bool_col = predicate.evaluate(&batch)?;

            let mask: Vec<bool> = bool_col
                .values
                .iter()
                .map(|v| matches!(v, ScalarValue::Bool(Some(true))))
                .collect();

            let filtered_columns: Vec<ColumnVector> = batch
                .columns
                .iter()
                .map(|col| {
                    let kept: Vec<ScalarValue> = col
                        .values
                        .iter()
                        .zip(mask.iter())
                        .filter(|&(_, keep)| *keep)
                        .map(|(val, _)| val.clone())
                        .collect();
                    ColumnVector::new(kept)
                })
                .collect();

            RecordBatch::new(batch.schema.clone(), filtered_columns)
        }))
    }

    fn children(&self) -> Vec<&dyn PhysicalPlan> {
        vec![self.input.as_ref()]
    }
}

/// Evaluates expressions to produce new columns for each batch.
pub struct ProjectionExec {
    pub input: Box<dyn PhysicalPlan>,
    pub schema: Schema,
    pub exprs: Vec<Box<dyn PhysicalExpr>>,
}

impl PhysicalPlan for ProjectionExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Box<dyn Iterator<Item = Result<RecordBatch, QueryError>> + '_> {
        let input_iter = self.input.execute();
        let exprs = &self.exprs;
        let schema = &self.schema;

        Box::new(input_iter.map(move |batch_result| {
            let batch = batch_result?;
            let columns: Result<Vec<ColumnVector>, _> =
                exprs.iter().map(|expr| expr.evaluate(&batch)).collect();
            RecordBatch::new(schema.clone(), columns?)
        }))
    }

    fn children(&self) -> Vec<&dyn PhysicalPlan> {
        vec![self.input.as_ref()]
    }
}
