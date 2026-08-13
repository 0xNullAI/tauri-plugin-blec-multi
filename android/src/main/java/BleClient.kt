package com.plugin.blec

import Peripheral
import android.Manifest
import android.annotation.SuppressLint
import android.app.Activity
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothProfile
import android.bluetooth.le.BluetoothLeScanner
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanFilter.Builder
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanResult.TX_POWER_NOT_PRESENT
import android.bluetooth.le.ScanSettings
import android.content.Context.MODE_PRIVATE
import android.content.Intent
import android.content.SharedPreferences
import android.content.pm.PackageManager
import android.net.Uri
import android.os.Build
import android.os.ParcelUuid
import android.provider.Settings
import android.util.Log
import android.util.SparseArray
import android.widget.Toast
import androidx.core.app.ActivityCompat
import androidx.core.app.ActivityCompat.startActivityForResult
import androidx.core.content.ContextCompat.getSystemService
import app.tauri.annotation.InvokeArg
import app.tauri.plugin.Channel
import app.tauri.plugin.Invoke
import app.tauri.plugin.JSArray
import app.tauri.plugin.JSObject
import java.util.Base64
import java.nio.charset.StandardCharsets

class BleDevice(
    val address: String,
    private val name: String,
    private val rssi: Int,
    private val connected: Boolean,
    private val bonded: Boolean,
    private val manufacturerData: SparseArray<ByteArray>?,
    private val serviceData: Map<ParcelUuid, ByteArray>?,
    private val services: List<ParcelUuid>?,
    private val txPowerLevel: Int?
){
    private val base64Encoder: Base64.Encoder = Base64.getEncoder()

    fun toJsObject():JSObject{
        val obj = JSObject()
        obj.put("address",address)
        obj.put("id",address)
        obj.put("name",name)
        obj.put("connected",connected)
        obj.put("bonded",bonded)
        obj.put("rssi",rssi)
        obj.put("txPowerLevel",txPowerLevel)
        // create Json Array from services
        val services = if (services != null) {
            val arr = JSArray()
            for (service in services){
                arr.put(service)
            }
            arr
        } else { null }
        obj.put("services",services)
        // crate object from sparse Array
        val manufacturerData = if (manufacturerData != null) {
            val subObj = JSObject()
            for (i in 0 until manufacturerData.size()) {
                val key = manufacturerData.keyAt(i)
                // get the object by the key.
                val value = manufacturerData.get(key)
                subObj.put(key.toString(),base64Encoder.encodeToString(value))
            }
            subObj
        } else { null }
        obj.put("manufacturerData",manufacturerData)
        // crate object from serviceData
        val serviceData = if (serviceData != null) {
            val subObj = JSObject()
            for ((key, value) in serviceData){
                subObj.put(key.toString(),base64Encoder.encodeToString(value))
            }
            subObj
        } else { null }
        obj.put("serviceData",serviceData)
        return obj
    }
}

class BleClient(private val activity: Activity, private val plugin: BleClientPlugin) {
    private var scanner: BluetoothLeScanner? = null
    private var adapter: BluetoothAdapter? = null
    private var manager: BluetoothManager? = null
    private var scanCb: ScanCallback? = null
    private var legacyScanCb: BluetoothAdapter.LeScanCallback? = null
    private val advertisedNames = mutableMapOf<String, String>()
    private val advertisedManufacturerData = mutableMapOf<String, SparseArray<ByteArray>>()

    private fun copyManufacturerData(source: SparseArray<ByteArray>): SparseArray<ByteArray> {
        val copy = SparseArray<ByteArray>(source.size())
        for (index in 0 until source.size()) {
            copy.put(source.keyAt(index), source.valueAt(index).clone())
        }
        return copy
    }

    /** Read AD type 0x09 exactly as the official DG-Lab scanner does. */
    private fun completeLocalName(record: ByteArray?): String? {
        if (record == null) return null
        var offset = 0
        while (offset < record.size) {
            val length = record[offset].toInt() and 0xff
            if (length == 0 || offset + length >= record.size) break
            val type = record[offset + 1].toInt() and 0xff
            if (type == 0x09 && length > 1) {
                return String(record, offset + 2, length - 1, StandardCharsets.UTF_8)
                    .trimEnd('\u0000')
                    .takeIf { it.isNotBlank() }
            }
            offset += length + 1
        }
        return null
    }

    /** Parse AD 0xFF from the raw compatibility-scan record. */
    private fun parseManufacturerData(record: ByteArray?): SparseArray<ByteArray>? {
        if (record == null) return null
        val result = SparseArray<ByteArray>()
        var offset = 0
        while (offset < record.size) {
            val length = record[offset].toInt() and 0xff
            if (length == 0 || offset + length >= record.size) break
            val type = record[offset + 1].toInt() and 0xff
            if (type == 0xff && length >= 3) {
                val companyId = (record[offset + 2].toInt() and 0xff) or
                    ((record[offset + 3].toInt() and 0xff) shl 8)
                result.put(companyId, record.copyOfRange(offset + 4, offset + length + 1))
            }
            offset += length + 1
        }
        return result.takeIf { it.size() > 0 }
    }

    /** Parse advertised 16-bit service UUIDs (AD 0x02/0x03). */
    private fun parseServiceUuids(record: ByteArray?): List<ParcelUuid>? {
        if (record == null) return null
        val result = mutableListOf<ParcelUuid>()
        var offset = 0
        while (offset < record.size) {
            val length = record[offset].toInt() and 0xff
            if (length == 0 || offset + length >= record.size) break
            val type = record[offset + 1].toInt() and 0xff
            if (type == 0x02 || type == 0x03) {
                var cursor = offset + 2
                val end = offset + length + 1
                while (cursor + 1 < end) {
                    val shortUuid = (record[cursor].toInt() and 0xff) or
                        ((record[cursor + 1].toInt() and 0xff) shl 8)
                    result.add(
                        ParcelUuid.fromString(
                            String.format("0000%04x-0000-1000-8000-00805f9b34fb", shortUuid)
                        )
                    )
                    cursor += 2
                }
            }
            offset += length + 1
        }
        return result.takeIf { it.isNotEmpty() }
    }

    private fun markFirstPermissionRequest(perm: String) {
        val sharedPreference: SharedPreferences =
            activity.getSharedPreferences("PREFS_PERMISSION_FIRST_TIME_ASKING", MODE_PRIVATE)
        sharedPreference.edit().putBoolean(perm, false).apply()
    }

    @InvokeArg
    class ScanParams {
        val services: ArrayList<String> = ArrayList()
        val onDevice: Channel? = null
        val allowIbeacons: Boolean = false
    }
    @SuppressLint("MissingPermission")
    fun startScan(invoke: Invoke) {
        // check if running
        if (scanCb != null){
            invoke.reject("Scan already running")
            return
        }
        val args = invoke.parseArgs(ScanParams::class.java)

        // get scanner
        if (scanner == null) {
            manager = getSystemService(activity, BluetoothManager::class.java)
                ?: throw RuntimeException("No bluetooth manager found")
            val bluetoothAdapter: BluetoothAdapter = manager!!.adapter
                ?: throw RuntimeException("No bluetooth adapter available")
            adapter = bluetoothAdapter
            // check if bluetooth is on
            if (!bluetoothAdapter.isEnabled ) {
                val enableBtIntent = Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE)
                startActivityForResult(activity, enableBtIntent,0,null)
            }
            scanner = bluetoothAdapter.bluetoothLeScanner
                ?: throw RuntimeException("No bluetooth scanner available for adapter")
        }

        // clear old devices
        this.plugin.devices.clear()
        advertisedNames.clear()
        advertisedManufacturerData.clear()

        var filters: ArrayList<ScanFilter?>? = null
        if (args.services.size > 0) {
            filters = ArrayList()
            for (uuid in args.services) {
                filters.add(Builder().setServiceUuid(ParcelUuid.fromString(uuid)).build())
            }
        }
        val settings = ScanSettings.Builder()
            .setCallbackType(ScanSettings.CALLBACK_TYPE_ALL_MATCHES)
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            // DG-Lab devices use legacy advertising. Several Android 11 OEM
            // stacks omit their scan response when extended-only scanning is
            // requested, which hides the AD 0x09 device name.
            .setLegacy(true)
            .build()

        // The official DG-Lab Android stack keeps a compatibility scanner for
        // Android 11 and older. Some MIUI Bluetooth stacks deliver the
        // advertising packet and scan response separately through the modern
        // API, losing AD 0x09 even though the old callback returns the merged
        // raw record. Use that platform compatibility path on those releases.
        if (Build.VERSION.SDK_INT <= Build.VERSION_CODES.R) {
            legacyScanCb = BluetoothAdapter.LeScanCallback { device, rssi, scanRecord ->
                val currentName = completeLocalName(scanRecord)
                    ?: device.name?.takeIf { it.isNotBlank() }
                    ?: device.alias?.takeIf { it.isNotBlank() }
                if (currentName != null) advertisedNames[device.address] = currentName
                val name = advertisedNames[device.address] ?: ""
                val connected = this@BleClient.manager!!.getConnectionState(
                    device,
                    BluetoothProfile.GATT_SERVER
                ) == BluetoothProfile.STATE_CONNECTED
                val bonded = device.bondState == BluetoothDevice.BOND_BONDED
                val bleDevice = BleDevice(
                    device.address,
                    name,
                    rssi,
                    connected,
                    bonded,
                    parseManufacturerData(scanRecord),
                    null,
                    parseServiceUuids(scanRecord),
                    null
                )
                this@BleClient.plugin.devices[bleDevice.address] =
                    Peripheral(this@BleClient.activity, device, this@BleClient.plugin)
                val res = JSObject()
                res.put("result", bleDevice.toJsObject())
                args.onDevice!!.send(res)
            }
            @Suppress("DEPRECATION")
            adapter?.startLeScan(legacyScanCb)
            invoke.resolve()
            return
        }

        scanCb = object: ScanCallback(){
            private fun sendResult(result: ScanResult){
                val currentName = completeLocalName(result.scanRecord?.bytes)
                    ?: result.scanRecord?.deviceName?.takeIf { it.isNotBlank() }
                    ?: result.device.name?.takeIf { it.isNotBlank() }
                    ?: if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                        result.device.alias?.takeIf { it.isNotBlank() }
                    } else null
                if (currentName != null) advertisedNames[result.device.address] = currentName
                val currentManufacturerData = result.scanRecord?.manufacturerSpecificData
                if (currentManufacturerData != null && currentManufacturerData.size() > 0) {
                    advertisedManufacturerData[result.device.address] =
                        copyManufacturerData(currentManufacturerData)
                }
                // A device often emits the name in a scan response and omits
                // it from the next advertising packet. Never erase a name we
                // already observed for the same address.
                val name = advertisedNames[result.device.address] ?: ""
                val debugManufacturer = advertisedManufacturerData[result.device.address]
                val debugManufacturerText = if (debugManufacturer == null) "-" else
                    (0 until debugManufacturer.size()).joinToString(",") { index ->
                        val key = debugManufacturer.keyAt(index)
                        val value = debugManufacturer.valueAt(index)
                        "$key:${value.joinToString("") { byte -> "%02x".format(byte) }}"
                    }
                val debugServices = result.scanRecord?.serviceUuids
                    ?.joinToString(",") { it.uuid.toString() } ?: "-"
                Log.d(
                    "BlecScanRaw",
                    "address=${result.device.address} rssi=${result.rssi} name=$name mfr=$debugManufacturerText services=$debugServices"
                )
                val connected = this@BleClient.manager!!.getConnectionState(result.device,BluetoothProfile.GATT_SERVER) == BluetoothProfile.STATE_CONNECTED
                val bonded = result.device.getBondState() == BluetoothDevice.BOND_BONDED
                val txPower = if (result.txPower == TX_POWER_NOT_PRESENT) {
                    null
                } else {
                    result.txPower
                }
                val device = BleDevice(
                    result.device.address,
                    name,
                    result.rssi,
                    connected,
                    bonded,
                    // Advertising and scan-response packets may arrive as
                    // separate callbacks. Preserve AD 0xFF just like the AD
                    // 0x09 name so a later partial packet cannot erase the
                    // official DG-Lab anonymous-device fallback signal.
                    advertisedManufacturerData[result.device.address],
                    result.scanRecord?.serviceData,
                    result.scanRecord?.serviceUuids,
                    txPower
                )
                this@BleClient.plugin.devices[device.address] = Peripheral(this@BleClient.activity, result.device, this@BleClient.plugin)
                val res = JSObject()
                res.put("result", device.toJsObject())
                args.onDevice!!.send(res)
            }
            override fun onBatchScanResults(results: List<ScanResult>){
                for(result in results){
                    sendResult(result)
                }
            }
            override fun onScanFailed(errorCode: Int){
                println("Scan failed with error code $errorCode")
            }
            override fun onScanResult(callbackType: Int, result: ScanResult){
                sendResult(result)
            }
        }
        scanner?.startScan(filters, settings, scanCb!!)
        invoke.resolve()
    }

    @SuppressLint("MissingPermission")
    fun stopScan(invoke: Invoke){
        legacyScanCb?.let { callback ->
            @Suppress("DEPRECATION")
            adapter?.stopLeScan(callback)
            legacyScanCb = null
        }
        if (scanCb!=null) {
            scanner?.stopScan(scanCb!!)
            scanCb = null
        }
        invoke.resolve()
    }

    fun adapterState(invoke: Invoke) {
        val response = JSObject()
        manager = getSystemService(activity, BluetoothManager::class.java)
        if (manager == null){
            response.put("result","unknown")
        } else {
            val adapter = manager?.adapter
            if (adapter == null){
                response.put("result","unknown")
            } else {
                // check if bluetooth is on
                if (adapter.isEnabled ) {
                    response.put("result","on")
                } else {
                    response.put("result","off")
                }
            }
        }

        invoke.resolve(response)
        return
    }
}
