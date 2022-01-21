import QtQuick 2.0
import pymmdt.app.pylib 1.0

Item {
    id: videoContent
    anchors.fill: parent
    
    Text {
        anchors.centerIn: parent
        text: "VIDEO: " + qsTr(user) + " " + qsTr(entry_name) + " " + qsTr(dtype)
    }

    ContentImage {
        height: _height
        width: _width
        image: content
    }
}
