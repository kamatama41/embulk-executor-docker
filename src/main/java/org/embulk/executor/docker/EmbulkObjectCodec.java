package org.embulk.executor.docker;

import com.github.kamatama41.nsocket.codec.ObjectCodec;
import org.embulk.config.ModelManager;

class EmbulkObjectCodec implements ObjectCodec {
    private final ModelManager modelManager;

    EmbulkObjectCodec(ModelManager modelManager) {
        this.modelManager = modelManager;
    }

    @Override
    public String encodeToJson(Object data) {
        return modelManager.writeObject(data);
    }

    @Override
    public <T> T decodeFromJson(String json, Class<T> valueType) {
        return modelManager.readObject(valueType, json);
    }
}
