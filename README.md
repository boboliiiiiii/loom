# Loom

Tarantool-backed config plane for Minecraft Paper plugins. Native Kotlin DSL with hot-reload via IPROTO_WATCH; a FUSE sidecar maps `/plugins/*/config.*` to Tarantool so legacy plugins keep using their normal IO.

## Usage

```kotlin
object PvpConfig : LoomSection("pvp", "config") {
    val maxRange   by config(100).inRange(1..1000)
    val cooldown   by config(60).reloadable()
                       .validate("must be 1..3600 sec") { it in 1..3600 }
    val pvpEnabled by config(true).reloadable()

    init {
        onChange("cooldown") { new -> rebuildCooldownMap() }
    }
}

class MyPlugin : JavaPlugin() {
    override fun onEnable() {
        PvpConfig.bind(Loom.client())
    }
}
```

## FUSE sidecar runtime requirements

```
# Set by the k8s pod spec, not in the image:
#   securityContext.capabilities.add: ["SYS_ADMIN"]   # required to mount FUSE
#   /dev/fuse exposed as a volumeDevice or hostPath CharDevice
#
# `privileged: true` is sufficient but heavier than necessary — prefer the
# narrow cap + device approach above.
```
