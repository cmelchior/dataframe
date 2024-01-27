package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.ColumnsSelector
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.RowExpression
import org.jetbrains.kotlinx.dataframe.aggregation.ColumnsForAggregateSelector
import org.jetbrains.kotlinx.dataframe.columns.ColumnReference
import org.jetbrains.kotlinx.dataframe.columns.toColumnSet
import org.jetbrains.kotlinx.dataframe.impl.aggregation.aggregators.Aggregators
import org.jetbrains.kotlinx.dataframe.impl.aggregation.aggregators.cast
import org.jetbrains.kotlinx.dataframe.impl.aggregation.comparableColumns
import org.jetbrains.kotlinx.dataframe.impl.aggregation.modes.aggregateAll
import org.jetbrains.kotlinx.dataframe.impl.aggregation.modes.aggregateFor
import org.jetbrains.kotlinx.dataframe.impl.aggregation.modes.aggregateOf
import org.jetbrains.kotlinx.dataframe.impl.aggregation.modes.of
import org.jetbrains.kotlinx.dataframe.impl.columns.toComparableColumns
import org.jetbrains.kotlinx.dataframe.impl.suggestIfNull
import org.jetbrains.kotlinx.dataframe.math.medianOrNull
import kotlin.reflect.KProperty
import org.jetbrains.kotlinx.dataframe.api.Interpolation
import org.jetbrains.kotlinx.dataframe.impl.aggregation.aggregators.cast2
import org.jetbrains.kotlinx.dataframe.math.Quantiles


/**
 *
 */
public enum class Interpolation {
    /**
     * i + (j - i) * fraction, where fraction is the fractional part of the index surrounded by i and j.
     *
     * Note, for very large [Long] values, this might return the wrong result because the values needs
     * to be converted to [Double]. If this is the case, use [LOWER], [HIGHER] or [NEAREST] instead.
     */
    LINEAR,

    /**
     * i
     */
    LOWER,

    /**
     * j
     */
    HIGHER,

    /**
     * i or j whichever is nearest.
     *
     * This is only available for [Number] types
     */
    NEAREST,

    /**
     * (i + j) / 2
     *
     * This is only available for [Number] types.
     * For very large [Long] values , this might return the wrong result because the values needs
     * to be converted to [Double]. If this is the case, use [LOWER], [HIGHER] or [NEAREST] instead.
     */
    MIDPOINT,
}

// region DataColumn

// For Numbers
public fun <T> DataColumn<T?>.quantile(q: Double, interpolation: Interpolation): Double where T: Comparable<T>, T: Number = quantileOrNull(q, interpolation).suggestIfNull("quantile")

// TODO Also add for BigInteger/BigDecimal?

// For all other comparables
public fun <T> DataColumn<T?>.quantile(q: Double, interpolation: Interpolation): Comparable<Any?> where T: Comparable<T> = quantileOrNull(q, interpolation).suggestIfNull("quantile")

public fun <T> DataColumn<T?>.quantileOrNull(q: Double, interpolation: Interpolation): Double? where T: Comparable<T>, T: Number = Aggregators.quantile(q, interpolation).cast2<T?, Double?>().aggregate(this)

public fun <T> DataColumn<T?>.quantileOrNull(q: Double, interpolation: Interpolation): Comparable<Any?>? where T: Comparable<T> = Aggregators.quantile(q, interpolation).cast2<T?, Comparable<Any?>?>().aggregate(this)

public inline fun <T, reified R : Comparable<R>> DataColumn<T>.quantileOrNull(q: Double, interpolation: Interpolation, noinline expression: (T) -> R?): R? =
    Aggregators.quantile(q, interpolation).cast<R?>().aggregateOf(this, expression)

public inline fun <T, reified R : Comparable<R>> DataColumn<T>.quantileOf(q: Double, interpolation: Interpolation, noinline expression: (T) -> R?): R =
    quantileOrNull(q, interpolation, expression).suggestIfNull("medianOf")

// endregion

// region DataRow

// TODO What to do with these ones. aggreateMixed is only available on mergedValues?
//public fun AnyRow.rowQuantileOrNull(q: Double, interpolation: Interpolation): Any? = Aggregators.quantile(q, interpolation).aggregateMixed(
//    values().filterIsInstance<Comparable<Any?>>().asIterable()
//)

//public fun AnyRow.rowMedian(): Any = rowMedianOrNull().suggestIfNull("rowMedian")
//
//public inline fun <reified T : Comparable<T>> AnyRow.rowMedianOfOrNull(): T? = valuesOf<T>().medianOrNull()
//
//public inline fun <reified T : Comparable<T>> AnyRow.rowMedianOf(): T =
//    rowMedianOfOrNull<T>().suggestIfNull("rowMedianOf")
//
//// endregion
//
// region DataFrame

public fun <T> DataFrame<T>.quantile(q: Double, interpolation: Interpolation): DataRow<T> = quantileFor(q, interpolation, comparableColumns())

public fun <T, C : Comparable<C>> DataFrame<T>.quantileFor(q: Double, interpolation: Interpolation, columns: ColumnsForAggregateSelector<T, C?>): DataRow<T> =
    Aggregators.quantile(q, interpolation).aggregateFor(this, columns)

public fun <T> DataFrame<T>.quantileFor(q: Double, interpolation: Interpolation, vararg columns: String): DataRow<T> = quantileFor(q, interpolation) { columns.toComparableColumns() }

public fun <T, C : Comparable<C>> DataFrame<T>.quantileFor(q: Double, interpolation: Interpolation, vararg columns: ColumnReference<C?>): DataRow<T> =
    quantileFor(q, interpolation) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> DataFrame<T>.quantileFor(q: Double, interpolation: Interpolation, vararg columns: KProperty<C?>): DataRow<T> =
    quantileFor(q, interpolation) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> DataFrame<T>.quantileFor(columns: ColumnsSelector<T, C?>): C =
    medianOrNull(columns).suggestIfNull("median")

//public fun <T> DataFrame<T>.quantile(q: Double, interpolation: Interpolation, vararg columns: String): Any = quantile(q, interpolation) { columns.toComparableColumns() }
//
//public fun <T, C : Comparable<C>> DataFrame<T>.quantile(q: Double, interpolation: Interpolation, vararg columns: ColumnReference<C?>): C =
//    quantile(q, interpolation) { columns.toColumnSet() }
//
//public fun <T, C : Comparable<C>> DataFrame<T>.quantile(q: Double, interpolation: Interpolation, vararg columns: KProperty<C?>): C =
//    quantile(q, interpolation) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> DataFrame<T>.quantileOrNull(q: Double, interpolation: Interpolation, columns: ColumnsSelector<T, C?>): C? =
    Aggregators.quantile(q, interpolation).aggregateAll(this, columns) as C?

public fun <T> DataFrame<T>.quantileOrNull(q: Double, interpolation: Interpolation, vararg columns: String): Any? = quantileOrNull(q, interpolation) { columns.toComparableColumns() }

public fun <T, C : Comparable<C>> DataFrame<T>.quantileOrNull(q: Double, interpolation: Interpolation, vararg columns: ColumnReference<C?>): C? =
    quantileOrNull(q, interpolation) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> DataFrame<T>.quantileOrNull(q: Double, interpolation: Interpolation, vararg columns: KProperty<C?>): C? =
    quantileOrNull(q, interpolation) { columns.toColumnSet() }

public inline fun <T, reified R : Comparable<R>> DataFrame<T>.quantileOf(q: Double, interpolation: Interpolation, crossinline expression: RowExpression<T, R?>): R? =
    Aggregators.quantile(q, interpolation).of(this, expression) as R?

// endregion

// region GroupBy

public fun <T> Grouped<T>.quantile(q: Double, interpolation: Interpolation): DataFrame<T> = quantileFor(q, interpolation, comparableColumns())

public fun <T, C : Comparable<C>> Grouped<T>.quantileFor(q: Double, interpolation: Interpolation, columns: ColumnsForAggregateSelector<T, C?>): DataFrame<T> =
    Aggregators.quantile(q, interpolation).aggregateFor(this, columns)

public fun <T> Grouped<T>.quantileFor(q: Double, interpolation: Interpolation, vararg columns: String): DataFrame<T> = quantileFor(q, interpolation) { columns.toComparableColumns() }

public fun <T, C : Comparable<C>> Grouped<T>.quantileFor(q: Double, interpolation: Interpolation, vararg columns: ColumnReference<C?>): DataFrame<T> =
    quantileFor(q, interpolation) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> Grouped<T>.quantileFor(q: Double, interpolation: Interpolation, vararg columns: KProperty<C?>): DataFrame<T> =
    quantileFor(q, interpolation) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> Grouped<T>.quantile(
    q: Double,
    interpolation: Interpolation,
    name: String? = null,
    columns: ColumnsSelector<T, C?>,
): DataFrame<T> = Aggregators.quantile(q, interpolation).aggregateAll(this, name, columns)

public fun <T> Grouped<T>.quantile(q: Double, interpolation: Interpolation, vararg columns: String, name: String? = null): DataFrame<T> =
    quantile(q, interpolation, name) { columns.toComparableColumns() }

public fun <T, C : Comparable<C>> Grouped<T>.quantile(
    q: Double, interpolation: Interpolation,
    vararg columns: ColumnReference<C?>,
    name: String? = null,
): DataFrame<T> = quantile(q, interpolation, name) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> Grouped<T>.quantile(q: Double, interpolation: Interpolation, vararg columns: KProperty<C?>, name: String? = null): DataFrame<T> =
    quantile(q, interpolation, name) { columns.toColumnSet() }

public inline fun <T, reified R : Comparable<R>> Grouped<T>.quantileOf(
    q: Double, interpolation: Interpolation,
    name: String? = null,
    crossinline expression: RowExpression<T, R?>,
): DataFrame<T> = Aggregators.quantile(q, interpolation).aggregateOf(this, name, expression)

// endregion

// region Pivot

public fun <T> Pivot<T>.quantile(q: Double, interpolation: Interpolation, separate: Boolean = false): DataRow<T> = quantileFor(q, interpolation, separate, comparableColumns())

public fun <T, C : Comparable<C>> Pivot<T>.quantileFor(
    q: Double, interpolation: Interpolation,
    separate: Boolean = false,
    columns: ColumnsForAggregateSelector<T, C?>,
): DataRow<T> = delegate { quantileFor(q, interpolation, separate, columns) }

public fun <T> Pivot<T>.quantileFor(q: Double, interpolation: Interpolation, vararg columns: String, separate: Boolean = false): DataRow<T> =
    quantileFor(q, interpolation, separate) { columns.toComparableColumns() }

public fun <T, C : Comparable<C>> Pivot<T>.quantileFor(
    q: Double, interpolation: Interpolation,
    vararg columns: ColumnReference<C?>,
    separate: Boolean = false,
): DataRow<T> = quantileFor(q, interpolation, separate) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> Pivot<T>.quantileFor(
    q: Double, interpolation: Interpolation,
    vararg columns: KProperty<C?>,
    separate: Boolean = false,
): DataRow<T> = quantileFor(q, interpolation, separate) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> Pivot<T>.quantile(q: Double, interpolation: Interpolation, columns: ColumnsSelector<T, C?>): DataRow<T> =
    delegate { quantile(q, interpolation, columns) }

public fun <T> Pivot<T>.quantile(q: Double, interpolation: Interpolation, vararg columns: String): DataRow<T> = quantile(q, interpolation) { columns.toComparableColumns() }

public fun <T, C : Comparable<C>> Pivot<T>.quantile(
    q: Double, interpolation: Interpolation,
    vararg columns: ColumnReference<C?>,
): DataRow<T> = quantile(q, interpolation) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> Pivot<T>.quantile(q: Double, interpolation: Interpolation, vararg columns: KProperty<C?>): DataRow<T> =
    quantile(q, interpolation) { columns.toColumnSet() }

//public inline fun <T, reified R : Comparable<R>> Pivot<T>.quantileOf(
//    q: Double, interpolation: Interpolation,
//    crossinline expression: RowExpression<T, R?>,
//): DataRow<T> = delegate { quantileOf(q, interpolation, expression) }

// endregion

// region PivotGroupBy

public fun <T> PivotGroupBy<T>.quantile(q: Double, interpolation: Interpolation, separate: Boolean = false): DataFrame<T> =
    quantileFor(q, interpolation, separate, comparableColumns())

public fun <T, C : Comparable<C>> PivotGroupBy<T>.quantileFor(
    q: Double, interpolation: Interpolation,
    separate: Boolean = false,
    columns: ColumnsForAggregateSelector<T, C?>,
): DataFrame<T> = Aggregators.quantile(q, interpolation).aggregateFor(this, separate, columns)

public fun <T> PivotGroupBy<T>.quantileFor(q: Double, interpolation: Interpolation, vararg columns: String, separate: Boolean = false): DataFrame<T> =
    quantileFor(q, interpolation, separate) { columns.toComparableColumns() }

public fun <T, C : Comparable<C>> PivotGroupBy<T>.quantileFor(
    q: Double, interpolation: Interpolation,
    vararg columns: ColumnReference<C?>,
    separate: Boolean = false,
): DataFrame<T> = quantileFor(q, interpolation, separate) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> PivotGroupBy<T>.quantileFor(
    q: Double, interpolation: Interpolation,
    vararg columns: KProperty<C?>,
    separate: Boolean = false,
): DataFrame<T> = quantileFor(q, interpolation, separate) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> PivotGroupBy<T>.quantile(q: Double, interpolation: Interpolation, columns: ColumnsSelector<T, C?>): DataFrame<T> =
    Aggregators.quantile(q, interpolation).aggregateAll(this, columns)

public fun <T> PivotGroupBy<T>.quantile(q: Double, interpolation: Interpolation, vararg columns: String): DataFrame<T> = quantile(q, interpolation) { columns.toComparableColumns() }

public fun <T, C : Comparable<C>> PivotGroupBy<T>.quantile(
    q: Double, interpolation: Interpolation,
    vararg columns: ColumnReference<C?>,
): DataFrame<T> = quantile(q, interpolation) { columns.toColumnSet() }

public fun <T, C : Comparable<C>> PivotGroupBy<T>.quantile(q: Double, interpolation: Interpolation, vararg columns: KProperty<C?>): DataFrame<T> =
    quantile(q, interpolation) { columns.toColumnSet() }

public inline fun <T, reified R : Comparable<R>> PivotGroupBy<T>.quantileFor(
    q: Double, interpolation: Interpolation,
    crossinline expression: RowExpression<T, R?>,
): DataFrame<T> = Aggregators.quantile(q, interpolation).aggregateOf(this, expression)

// endregion

