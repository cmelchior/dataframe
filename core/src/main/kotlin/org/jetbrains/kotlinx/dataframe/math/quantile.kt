package org.jetbrains.kotlinx.dataframe.math

import java.math.BigDecimal
import java.math.BigInteger
import org.jetbrains.kotlinx.dataframe.api.Interpolation
import org.jetbrains.kotlinx.dataframe.impl.asList
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.typeOf

/**
 * TODO
 */
@JvmName("quantileBigDecimal")
public inline fun Iterable<BigDecimal>.quantile(
    q: Double,
    interpolation: Interpolation = Interpolation.LINEAR
): BigDecimal? = quantile(listOf(q), interpolation).first()

/**
 * TODO
 */
@JvmName("quantileBigDecimals")
public inline fun Iterable<BigDecimal>.quantile(
    qs: List<Double>,
    interpolation: Interpolation = Interpolation.LINEAR
): List<BigDecimal?> = Quantiles(qs, interpolation).computeFromBigDecimals(this)


/**
 * TODO
 */
@JvmName("quantileBigInteger")
public inline fun Iterable<BigInteger>.quantile(
    q: Double,
    interpolation: Interpolation = Interpolation.LINEAR
): BigDecimal? = quantile(listOf(q), interpolation).first()

/**
 * TODO
 */
@JvmName("quantileBigIntegers")
public inline fun Iterable<BigInteger>.quantile(
    qs: List<Double>,
    interpolation: Interpolation = Interpolation.LINEAR
): List<BigDecimal?> = Quantiles(qs, interpolation).computeFromBigIntegers(this)

/**
 * TODO
 */
@JvmName("quantileNumber")
public inline fun <reified T> Iterable<T>.quantile(
    q: Double,
    interpolation: Interpolation = Interpolation.LINEAR
): Double? where T: Comparable<T>, T: Number = quantile(listOf(q), interpolation).first()

/**
 * TODO
 */
@JvmName("quantileNumbers")
public inline fun <reified T> Iterable<T>.quantile(
    qs: List<Double>,
    interpolation: Interpolation = Interpolation.LINEAR
): List<Double?> where T: Comparable<T>, T: Number = Quantiles(qs, interpolation).computeFromNumbers(this, typeOf<T>())


/**
 * TODO
 *
 * @throws IllegalArgumentException if a non-supported [Interpolation] type is used.
 */
@JvmName("quantileComparable")
public inline fun <reified T: Comparable<T>> Iterable<T>.quantile(
    q: Double,
    interpolation: Interpolation = Interpolation.LOWER
): T? = quantile(listOf(q), interpolation).first()

/**
 * TODO
 *
 * @throws IllegalArgumentException if a non-supported [Interpolation] type is used.
 */
@JvmName("quantileComparables")
public inline fun <reified T: Comparable<T>> Iterable<T>.quantile(
    qs: List<Double>,
    interpolation: Interpolation = Interpolation.LOWER
): List<T?> = Quantiles(qs, interpolation).computeFromComparables(this, typeOf<T>())


/**
 * Description of how calculations
 */
//@PublishedApi
public class Quantiles(private val quantiles: List<Double>, private val interpolation: Interpolation) {

    public constructor(quantile: Double, interpolation: Interpolation): this(listOf(quantile), interpolation)

    // Whether to sort incoming data or not.
    // If sorted, data will be copied and sorted, which will result in twice the memory usage.
    // If data isn't sorted, finding quantiles is O(N) (using QuickSelect)
    private val sortInput: Boolean = (quantiles.size > 1)

    init {
        quantiles.forEach { q ->
            if (q !in 0.0..1.0) {
                throw IllegalArgumentException("Quantiles must be within [0.0, 1.0]. It was: $q")
            }
        }
    }

    /**
     * Generic method for calculating quantiles for types that might not be known at compile type.
     * It will automatically redirect to one of the more appropriate method on this class depending
     * on the [type].
     */
    public fun <T: Comparable<*>> compute(data: Iterable<T?>, type: KType): List<Comparable<Any?>?> {
        println("Type: $type")
        @Suppress("UNCHECKED_CAST")
        return when(type.classifier) {
            Byte::class -> computeFromNumbers(data as Iterable<Byte>, type) as List<Comparable<Any?>>
            Short::class -> computeFromNumbers(data as Iterable<Short>, type) as List<Comparable<Any?>>
            Int::class -> computeFromNumbers(data as Iterable<Int>, type) as List<Comparable<Any?>>
            Long::class -> computeFromNumbers(data as Iterable<Long>, type) as List<Comparable<Any?>>
            Float::class -> computeFromNumbers(data as Iterable<Float>, type) as List<Comparable<Any?>>
            Double::class -> computeFromNumbers(data as Iterable<Double>, type) as List<Comparable<Any?>>
            BigInteger::class -> computeFromBigIntegers(data as Iterable<BigInteger>) as List<Comparable<Any?>>
            BigDecimal::class -> computeFromBigDecimals(data as Iterable<BigDecimal>) as List<Comparable<Any?>>
            else -> computeFromComparables(data.filterNotNull() as Iterable<Comparable<Any?>>, type) as List<Comparable<Any?>>
        }
    }

    /**
     * Method for calculating the quantiles for "simple" number types: [Byte], [Short], [Int], [Long],
     * [Float] and [Double]. Note, that not all [Long] values can be correctly represented as [Double] so for
     * [Long] values consisting of more than 53 bit, the returned result might not be accurate.
     */
    public fun <T> computeFromNumbers(data: Iterable<T>, type: KType): List<Double?> where T: Comparable<T>, T: Number {
        val (list, getElementAt) = prepareData(data, type)
        // Define the function that cal quantile, based on type and interpolation
        val calcFunc: (Double, Int, Int) -> Double = { position: Double, lowerIndex: Int, higherIndex: Int ->
            when (interpolation) {
                Interpolation.LINEAR -> {
                    if (lowerIndex == higherIndex) {
                        getElementAt(list, lowerIndex).toDouble()
                    } else {
                        val lower: T = getElementAt(list, lowerIndex)
                        val higher: T = getElementAt(list, higherIndex)
                        var fraction: Double = position - lowerIndex

                        // All 32-bit numbers can safely be cast to Double, 64-bit numbers can only represent the first
                        // 53 bit accurately in a Double, so for those types, we try to stay delay converting to Double.
                        when (type.classifier) {
                            Byte::class -> (lower as Byte) + ((higher as Byte) - (lower as Byte)) * fraction
                            Short::class -> (lower as Short) + ((higher as Short) - (lower as Short)) * fraction
                            Int::class -> (lower as Int) + ((higher as Int) - (lower as Int)) * fraction
                            Long::class -> (lower as Long) + ((higher as Long) - (lower as Long)) * fraction
                            Float::class -> (lower as Float) + ((higher as Float) - (lower as Float)) * fraction
                            Double::class -> (lower as Double) + ((higher as Double) - (lower as Double)) * fraction
                            Number::class -> lower.toDouble() + (higher.toDouble() - lower.toDouble()) * fraction
                            else -> throw IllegalStateException("Type not supported: $type")
                        }
                    }
                }
                Interpolation.LOWER -> getElementAt(list, lowerIndex).toDouble()
                Interpolation.HIGHER -> getElementAt(list, higherIndex).toDouble()
                Interpolation.NEAREST -> getElementAt(list, Math.round(position).toInt()).toDouble()
                Interpolation.MIDPOINT -> {
                    if (lowerIndex == higherIndex) {
                        getElementAt(list, lowerIndex).toDouble()
                    } else {
                        val lower: T = getElementAt(list, lowerIndex)
                        val higher: T = getElementAt(list, higherIndex)
                        when (type.classifier) {
                            Byte::class -> ((lower as Byte) + (higher as Byte)) / 2.0
                            Short::class -> ((lower as Short) + (higher as Short)) / 2.0
                            Int::class -> ((lower as Int) + (higher as Int)) / 2.0
                            Long::class -> ((lower as Long) + (higher as Long)) / 2.0
                            Float::class -> ((lower as Float) + (higher as Float)) / 2.0
                            Double::class -> ((lower as Double) + (higher as Double)) / 2.0
                            else -> throw IllegalStateException("Type not supported: $type")
                        }
                    }
                }
            }
        }
        return computeQuantiles(list, calcFunc)
    }

    public fun computeFromBigIntegers(data: Iterable<BigInteger>): List<BigDecimal?> {
        return computeFromBigNumbers(data, BigInteger::class.createType())
    }

    public fun computeFromBigDecimals(data: Iterable<BigDecimal>): List<BigDecimal?> {
        return computeFromBigNumbers(data, BigDecimal::class.createType())
    }

    /**
     * Method for calculating quantiles for types we are not able to run calculations on.
     * This also mean they only support a subset of the Interpolation methods.
     */
    public  fun <T: Comparable<T>> computeFromComparables(data: Iterable<T>, type: KType): List<T?> {
        val (list, getElementAt) = prepareData(data, type)
        // Define the function that cal quantile, based on type and interpolation
        val calcFunc: (Double, Int, Int) -> T = { position: Double, lowerIndex: Int, higherIndex: Int ->
            when(interpolation) {
                Interpolation.LOWER -> getElementAt(list, lowerIndex)
                Interpolation.HIGHER -> getElementAt(list, higherIndex)
                Interpolation.NEAREST -> getElementAt(list, Math.round(position).toInt())
                else -> throw IllegalArgumentException("$interpolation is not available for this type: $type")
            }
        }
        return computeQuantiles(list, calcFunc)
    }

    // Helper method, making sure that all quantile calculations are validated and set up the same way.
    private fun <T: Comparable<T>> prepareData(data: Iterable<T>, type: KType): Pair<List<T>, (List<T>, Int) -> T> {
        // QuickSelect only works on data of the same type, so for `Number`, always sort it by converting
        // values to Doubles
        val sort = sortInput || type.classifier == Number::class
        val list: List<T> = if (sort) {
            when(type.classifier) {
                Number::class -> data.sortedBy { (it as Number).toDouble() }
                else -> data.sorted()
            }
        } else {
            data.asList()
        }
        val getElementFunc: (List<T>, Int) -> T = if (sort) {
            { list: List<T>, index: Int -> list[index] }
        } else {
            { list: List<T>, index: Int -> list.quickSelect(index) }
        }
        return Pair(list, getElementFunc)
    }

    // Helper method for calculating quantiles from lists of BigInteger/BigDecimal.
    // Only these two types are supported when using this method.
    private fun <T> computeFromBigNumbers(data: Iterable<T>, type: KType): List<BigDecimal?> where T: Comparable<T>, T: Number {
        val (list, getElementAt) = prepareData(data, type)
        // Define the function that cal quantile, based on type and interpolation
        val calcFunc: (Double, Int, Int) -> BigDecimal = { position: Double, lowerIndex: Int, higherIndex: Int ->
            val result: Number = when(interpolation) {
                Interpolation.LINEAR -> {
                    if (lowerIndex == higherIndex) {
                        getElementAt(list, lowerIndex)
                    } else {
                        val lower: T = getElementAt(list, lowerIndex)
                        val higher: T = getElementAt(list, higherIndex)
                        var fraction: Double = position - lowerIndex
                        when (type.classifier) {
                            BigInteger::class -> BigDecimal((lower as BigInteger)) + BigDecimal(((higher as BigInteger) - (lower as BigInteger))) * BigDecimal(fraction)
                            BigDecimal::class -> (lower as BigDecimal) + ((higher as BigDecimal) - (lower as BigDecimal)) * BigDecimal(fraction)
                            else -> throw IllegalStateException("Type not supported: $type")
                        }
                    }
                }
                Interpolation.LOWER -> getElementAt(list, lowerIndex)
                Interpolation.HIGHER -> getElementAt(list, higherIndex)
                Interpolation.NEAREST -> getElementAt(list, Math.round(position).toInt())
                Interpolation.MIDPOINT -> {
                    if (lowerIndex == higherIndex) {
                        getElementAt(list, lowerIndex)
                    } else {
                        val lower: T = getElementAt(list, lowerIndex)
                        val higher: T = getElementAt(list, higherIndex)
                        when (type.classifier) {
                            BigInteger::class -> BigDecimal(((lower as BigInteger) + (higher as BigInteger))) / BigDecimal(2.0)
                            BigDecimal::class -> ((lower as BigDecimal) + (higher as BigDecimal)) / BigDecimal(2.0)
                            else -> throw IllegalStateException("Type not supported: $type")
                        }
                    }
                }
            }
            when(result) {
                is BigDecimal -> result
                is BigInteger -> BigDecimal(result)
                else -> throw IllegalStateException("Type not supported: $type")
            }
        }
        return computeQuantiles(list, calcFunc)
    }

    private fun <T, R> computeQuantiles(list: List<T>, calcFunc: (Double, Int, Int) -> R): List<R?> {
        return quantiles.map { q: Double ->
            val position: Double = (list.size - 1) * q
            val lowerIndex = position.toInt()
            val higherIndex = (position + 1).toInt().coerceIn(0, list.size - 1)
            calcFunc(position, lowerIndex, higherIndex)
        }
    }

}

