import QtQuick 2.0
import chimerapy.app.pylib 1.0

Item {
    id: videoContent
    anchors.fill: parent
    
    Text {
        id: entryTitle
        anchors.top: parent.top
        text: qsTr(user)+"\\" + qsTr(entry_name)
        font.pointSize: 12
        color: "white"
    }

    ContentImage {
        anchors.top: entryTitle.bottom
        height: _height
        width: _width
        image: content
    }
}
