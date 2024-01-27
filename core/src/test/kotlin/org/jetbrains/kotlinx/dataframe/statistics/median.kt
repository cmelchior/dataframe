package org.jetbrains.kotlinx.dataframe.statistics

import io.kotest.matchers.shouldBe
import org.jetbrains.kotlinx.dataframe.api.Infer
import org.jetbrains.kotlinx.dataframe.api.columnOf
import org.jetbrains.kotlinx.dataframe.api.dataFrameOf
import org.jetbrains.kotlinx.dataframe.api.mapToColumn
import org.jetbrains.kotlinx.dataframe.api.median
import org.jetbrains.kotlinx.dataframe.api.rowMedian
import org.junit.Test

class MedianTests {

    @Test
    fun `median of two columns`() {
        val df = dataFrameOf("a", "b")(
            null, null,
            null, null,
            null, null,
            null, null,
        )
        df.median("a", "b") shouldBe 7
    }

    @Test
    fun `median of Short values`() {
        val df = dataFrameOf("a", "b")(
            1.toShort(), 4.toShort(),
            2.toShort(), 6.toShort(),
            7.toShort(), 7.toShort()
        )
        df.median("a", "b") shouldBe 4
    }

    @Test
    fun `row median`() {
        val df = dataFrameOf("a", "b")(
            1, 3,
            2, 4,
            7, 7
        )
        df.mapToColumn("", Infer.Type) { it.rowMedian() } shouldBe columnOf(2, 3, 7)
    }

    // Add other median tests
}
