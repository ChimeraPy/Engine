import QtQuick 2.0

// Custom Imports Items
import "../diagrams"

Item {
    id: pageRoot

    Rectangle {
        id: rectangle
        color: "#44475a"
        anchors.fill: parent

        Text {
            anchors.fill: parent
            horizontalAlignment: Text.AlignHCenter
            verticalAlignment: Text.AlignVCenter
            color: "#f8f8f2"
            text: qsTr("Home Page")
        }

    }
}

/*##^##
Designer {
    D{i:2;anchors_height:160;anchors_width:813;anchors_x:154;anchors_y:132}D{i:1;anchors_height:400;anchors_width:1000;anchors_x:0;anchors_y:0}
}
##^##*/
