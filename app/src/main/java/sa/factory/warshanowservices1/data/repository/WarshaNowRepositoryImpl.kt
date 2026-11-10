package sa.factory.warshanowservices1.data.repository

import sa.factory.warshanowservices1.domain.repository.WarshaNowRepository
import sa.factory.warshanowservices1.hardware.HardwareSensorDriver
import sa.factory.warshanowservices1.model.WarshaNowItem
import sa.factory.warshanowservices1.model.WarshaNowOrder
import sa.factory.warshanowservices1.model.WarshaNowTelemetry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch

class WarshaNowRepositoryImpl(
    private val sensorDriver: HardwareSensorDriver
) : WarshaNowRepository {

    private val _catalog = MutableStateFlow<List<WarshaNowItem>>(listOf(
        WarshaNowItem("1", "حزمة الهوية الأساسية", "تصميم شعار ثنائي اللغة مع دليل استخدام كامل", 1500.0, "أساسي", "تصميم الشعار"),
        WarshaNowItem("2", "تطبيق الهوية الرقمية", "دمج NFC للتحقق من صحة العلامة التجارية عبر الأجهزة", 2800.0, "تقني", "الهوية المرئية"),
        WarshaNowItem("3", "نظام التتبع الجغرافي", "ربط GPS لتفعيل الخدمات الموقعية للعلامة", 950.0, "جديد", "إرشادات العلامة")
    ))
    override val catalogFlow: Flow<List<WarshaNowItem>> = _catalog.asStateFlow()

    private val _cart = MutableStateFlow<List<WarshaNowItem>>(emptyList())
    override val activeCart: StateFlow<List<WarshaNowItem>> = _cart.asStateFlow()

    override val telemetryFlow: Flow<WarshaNowTelemetry> = sensorDriver.observePeripheralStream()
        .map { frame ->
            WarshaNowTelemetry(
                peripheralName = frame.peripheralName,
                status = frame.connectionState,
                liveReading = frame.livePayload,
                batteryPct = 96,
                signalDbm = -54
            )
        }

    override fun addToCart(item: WarshaNowItem) {
        val current = _cart.value.toMutableList()
        current.add(item)
        _cart.value = current
    }

    override fun removeFromCart(item: WarshaNowItem) {
        val current = _cart.value.toMutableList()
        current.remove(item)
        _cart.value = current
    }

    override fun clearCart() {
        _cart.value = emptyList()
    }

    override suspend fun checkout(paymentMethod: String): WarshaNowOrder {
        val currentItems = _cart.value.toList()
        val subtotal = currentItems.sumOf { it.priceSar }
        val vat = subtotal * 0.15
        val total = subtotal + vat
        val order = WarshaNowOrder(
            orderId = "ORD-${System.currentTimeMillis() % 100000}",
            items = currentItems,
            subtotalSar = subtotal,
            vatSar = vat,
            totalSar = total,
            paymentMethod = paymentMethod
        )
        clearCart()
        return order
    }

    override suspend fun refreshCatalog() {
        // Offline-first sync logic
    }
}
