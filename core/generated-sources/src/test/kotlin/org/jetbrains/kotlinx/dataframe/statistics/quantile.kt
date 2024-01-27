package org.jetbrains.kotlinx.dataframe.statistics

import org.jetbrains.kotlinx.dataframe.api.Interpolation
import org.jetbrains.kotlinx.dataframe.api.column
import org.jetbrains.kotlinx.dataframe.api.dataFrameOf
import org.jetbrains.kotlinx.dataframe.api.medianOf
import org.jetbrains.kotlinx.dataframe.api.quantile
import org.jetbrains.kotlinx.dataframe.math.Quantiles
import org.junit.Test
import kotlin.reflect.full.createType

class QuantileTests {


    @Test
    fun test() {
//        val a by column<Comparable<Any?>>()
        val list: List<Comparable<*>> = listOf(
            1,
            2L,
            3.0f,
            4.0,
        )

        val df = dataFrameOf("a")(
            1.0, 1
        )
        val a by column<Number>()

        val result = df[a].medianOf { it.toDouble() }


//            val result: List<Comparable<Any?>?> = Quantiles(0.5, Interpolation.HIGHER).compute(list, Number::class.createType())
//        println(result.joinToString())
    }
}
